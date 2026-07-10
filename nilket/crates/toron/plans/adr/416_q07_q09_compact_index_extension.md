# ADR 416 - q07/q09 compact spilled-index extension (second seam)

## Context

The 415 board's ranked follow-on list starts here: **q07 and q09 fail with byte-identical pre-414 index payloads**
(`streamed inner join build spill resident state`, requested 1,074,443,016 / available 1,073,741,824,
`resident_index_bytes=1,074,377,480`) - the exact wall 414's compact index removed for q12, **on a different creation
path**. Two cells, one shared producer, the strongest prior proof on the board.

**The seam (grounded):** 414 landed the compact index behind `compact_index_enabled`: enabled builds construct
`SpilledBatchIndex::for_key_plan(...)` (`exec/join/spill.rs:1251`/`:1303`); disabled builds still append into the
legacy `HashMap<JoinKey, Vec<SpilledBatchRowRef>>` (`:1262`/`:1323`). 414 opted in q12's streamed-spilled build
helpers; **q07/q09 route through a creation path where the flag is still off**. A bounded in-place rescue also exists
(`:965`, `COMPACT_SPILL_INDEX_MAX_RETAINED_CAP_OVERSHOOT_BYTES`), but q07/q09's 701,192-byte overshoot evidently
exceeds it - and widening the rescue window would be a cap bump in disguise. The honest fix is **enabling the compact
representation from the start on their path**, exactly as q12's.

**Operator tightening #1 - no inherited headroom:** q12's 2.001x accounted (~6.7x real) reduction does **not**
transfer by assertion. 416 must prove, on q07 and q09 specifically: (a) the compact representation is enabled
**before the first append** (not a mid-build rescue conversion, not a partial legacy index), and (b) the **measured**
compact `resident_index_bytes` on these builds, recorded directly.

**Operator tightening #2 - flag retirement is out of scope** unless Phase 0 proves *every* creation path is
compact-safe. If any path legitimately needs the legacy representation (or is unproven), the
`compact_index_enabled`-retirement cleanup is **named separately**, not folded in.

## Decision

### Phase 0 - creation-path census (before code)
1. **Identify q07/q09's creation site(s):** which constructor leaves `compact_index_enabled` off, and whether that is
   a deliberate fence (semantic reason) or an unopted omission. Confirm both queries route identically (their
   byte-identical payloads say yes) and that their key plans are primitive (compact-eligible).
2. **Enumerate all `SpilledInnerJoinBuild` creation paths** and each one's flag state + compact-safety - this census
   is the evidence base for the retirement decision (here only to *name* it; retirement itself is a separate cleanup
   unless every path is proven safe and the operator opts to fold).
3. Record before code.

### The cut - opt in the second seam
- Enable the compact index at q07/q09's creation path for primitive key plans; `Generic`/null-safe keep the fallback
  (the 414 fence). Likely a few lines riding 414's entire structure, tests, and guards.
- **Enabled-before-first-append proof:** a discriminating test asserting the build's index is the compact
  representation with **zero legacy entries** before the first `write_batch_chunks` on this path - it must fail if
  the path stays legacy or only converts via the rescue.
- **Direct accounting on the drivers:** record the measured compact `resident_index_bytes` for the q07 and q09 builds
  (component payload via a low-cap probe or telemetry observation) - the headroom claim is per-build measurement, not
  q12 inheritance.
- No cap/multiplier/scope change; no qNN branch; no rescue-window widening.

### Stop-lines
- **Semantic fence discovered** (the path is legacy for a reason) -> seal the finding + design accordingly; no
  blind flag-flip.
- **Flag retirement** -> only as a separately named cleanup unless Phase 0 proves all paths compact-safe.

## Acceptance
- **Phase 0 recorded:** the creation-site identification (omission vs fence), the all-paths census, key-plan
  eligibility - before code.
- **Primary: q07 and q09 both move off the resident-index wall** - persisted `pass@compare` each, **or a named +
  reproduced next wall** (both were never-probed cells; q09 already peaks at 3,457.7 MiB *at the wall point*, deep in
  the q05/q20 borderline zone - an OOM behind the index wall is a live possibility and is recorded honestly, not
  rounded up).
- **Enabled-before-first-append proven** (the discriminating test) and **measured per-build compact
  `resident_index_bytes` recorded for both drivers** - no inherited headroom.
- **414's apparatus carried:** fidelity (accounted == computed), guard-armed, lookup-equality, truncation-bounds, and
  fence tests all green.
- **Controls:** the spill-bearing greens re-proven (`q03 q10 q12 q14 q16 q19 q20` pass@compare; forensic spill bytes
  in band); remaining greens (`q01 q04 q06 q08 q15`) cheap-optional; q05 stays red (not a control); q20's residency
  watch noted if its peak moves.
- **No cap/scope change; full engine gate** (`--no-fail-fast` both crates + architecture after fmt); de-branded; no
  committed `.perf.data`.
- **Scope honesty:** two cells on one shared producer; the remaining wall families (scan-output x4, q02 join-output,
  q22 writer-resident, q05 residency, q11 oracle) stay named; flag retirement named separately unless proven-safe.

## Guardrails
- **Measured, not inherited** - per-build accounting on both drivers; q12's ratio is prior, not proof.
- **Compact from the first append** - the rescue path is not the mechanism; the test discriminates.
- **No rescue-window widening** - overshoot constants are frozen.
- **The 414 fence holds** - primitive plans compact; Generic/null-safe fallback.
- **Retirement separate** - the census informs it; 416 does not fold it without proof + operator opt-in.
- **Won ground is a control** - spill-bearing greens + forensic spill bytes.

## Risks
- **A wall behind the wall** (q09's 3.4 GiB at failure) -> reach-board honesty: name + reproduce; possibly the q05
  residency family.
- **The path is fenced for a reason** -> stop-line; finding sealed.
- **Compact index still over cap on these builds** (bigger builds than q12's) -> honest measurement either way; if
  compact-yet-over, the next mechanism (sharded/deferred index) is named with evidence - not a cap tweak.
- **Sibling regression** -> 414 test apparatus + spill-bearing controls + full gates.

## Forward Arc
q07/q09 landing (or honestly advancing) starts the next arc's producer-ranked sweep: scan-output streaming
(q13/q17/q18/q21), q02 join-output, q22 writer residency, q05 peak-residency diagnosis, q11 oracle reconciliation -
then the speed thread. If the Phase 0 census proves all creation paths compact-safe, the
`compact_index_enabled`-retirement cleanup (flag + legacy index both retire) is named as its own small cut.

## Grounding evidence (file:line)
Flag + representations: `exec/join/spill.rs:1251`/`:1303` (`for_key_plan` opt-in sites), `:1262`/`:1323` (legacy
`append_spilled_batch_row_ref`), `:965` (bounded rescue + `COMPACT_SPILL_INDEX_MAX_RETAINED_CAP_OVERSHOOT_BYTES`),
`:572`/`:577` (Generic/null-safe fallback). 414 apparatus: compact structure, `accounted_bytes`/`computed_bytes`,
guard-armed/fence/truncation tests (`plans/impl/414_q12_spilled_index_compaction_results.md`). q07/q09 walls:
415 board - q07 3,752 ms / 1,664.7 MiB, q09 7,050 ms / 3,457.7 MiB, both
`requested 1,074,443,016 / available 1,073,741,824`, `resident_index_bytes=1,074,377,480`
(`plans/impl/415_sf10_full_board_live_sweep_results.md`).
