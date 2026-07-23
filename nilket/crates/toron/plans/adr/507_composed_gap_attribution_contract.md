# ADR 507 - composed-gap attribution: define the contract before extending coverage

The sf=8 board (`impl/qseries_repin_sf8_20260723.md`) put composed queries at a median 6.38x DuckDB-1t
against the primitive ladder's ~2.4x, and split the recoverable time into two mechanism classes plus a
correctness cell that is now fixed. It did not say *why* any cell is slow. This ADR opens the
composed-gap decomposition cycle, and its first deliverable is the measuring instrument, not a kernel.

The candidate set is **seven** queries:

- **spill-heavy:** q17 (46.28x, 623 MB), q18 (10.70x, 990 MB), q21 (6.38x, 898 MB)
- **non-spill composition:** q02 (16.99x), q08 (9.56x), q09 (6.52x), q20 (14.94x) — all zero spill

None is excluded. Encoded columns remain a candidate mechanism, ranked by measured attributable share
rather than assumed.

## The problem this cycle has to solve first

Toron has one stage-attribution instrument, `ChainStageProfiler`
(`TORON_CHAIN_STAGE_PROFILE_PATH`). Probed at sf=8, it emits for **q21 only**; q17, q18 and q20 all
ran green and produced nothing, because the profiler is bound to
`try_streaming_inner_prefix_semi_anti_scan_probe_base` — the reoriented grouped-chain path. Six of the
seven candidates have no stage-level attribution at all.

Where it does fire it is decisive. q21 at sf=8:

| stage | ms | share of attributed |
|---|---:|---:|
| base_scan | 622 | 6% |
| inner_replay_0 | 1,410 | 13% |
| **inner_replay_1** | **8,028** | **76%** |
| inner_replay_2 | 456 | 4% |
| semi/anti/grouped_fold | 225 | 2% |

10.7 s attributed against ~17.4 s execute. The ~6.6 s difference is real work the stage timers do not
cover — spill reads, page decode, index builds — and naming it is part of the contract below.

**The risk this ADR exists to prevent.** The obvious move is to copy the chain profiler into the other
paths. The chain model assumes a replay-structured lifecycle: numbered stages, a build reservoir, a
probe reservoir, a terminal fold. q17 and q20 do not have that shape, and a stage vocabulary borrowed
from a lifecycle they do not share would produce numbers that look comparable and are not — the same
failure mode as attributing a shared primitive across two domains by symbol. Define what a stage *is*
before deciding where stages go.

## Decision

Run the cycle in six ordered steps. Each is a gate: a step that fails stops the cycle rather than
handing a weaker instrument to the next one.

**1. Define the attribution contract.** A written contract, pinned by tests, covering:
- *stage identity* — what makes a stage one stage, and what makes two stages in different execution
  paths comparable (or explicitly not);
- *inclusive vs exclusive timing* — nested stages must state which they report, so shares sum;
- *rows in / rows out* per stage;
- *spill reads and writes* per stage, split by side (the existing chain profile already splits
  `build_spill_bytes` from `probe_spill_bytes`);
- *page and index work* — page reads, index builds, decoded bytes;
- *explicit unattributed time* — the residual is a first-class field, not a subtraction the reader
  performs. An instrument that cannot say what it missed cannot be trusted about what it caught;
- *explicit unattributed bytes* — the same discipline for I/O. The contract first fixes the byte
  *accounting domains* — which counter measures what: whole-query spill written, per-stage spill
  written by side (`build_spill_bytes` / `probe_spill_bytes`), physical page reads, decoded bytes —
  and states which domain each stage field belongs to and which domains are expected to sum to which
  whole. A residual is reported *within a named domain*, never as a bare byte difference. This is the
  distinction that makes step 2 answerable: a leftover count is either unattributed I/O in one domain
  or two counters that were never scoped to sum, and the contract must force that call rather than let
  a number float between domains.

**2. Reconcile q21's accounting before trusting the instrument.** The profile run's whole-query spill
is 897,713,384 bytes, identical to the board's; the per-stage `build_spill_bytes` (24,317,328) plus
`probe_spill_bytes` (873,395,520) is 897,712,848, leaving **536 bytes**. The earlier "24 MB vs 898 MB"
reading was a component quoted as a total, not an instrument defect. But the 536 bytes may not be
unattributed I/O at all — the per-stage counters and the whole-query counter may simply be scoped to
different domains (e.g. one counts a page header the other does not), in which case they were never
required to sum. Step 2 is not passed until that call is made against step 1's accounting domains: the
residual is either named as unattributed bytes *in a stated domain*, or the two counters are declared
non-summing with the reason. A bare "close to zero" is not a reconciliation.

**3. Pilot coverage on q17 and q20.** One from each mechanism class, both non-chain. This is the
discriminating test of whether one abstraction spans both classes. If q17 and q20 need incompatible
stage vocabularies, that is a finding: the contract admits two profiles with an explicit
non-comparability rule, rather than one vocabulary that quietly means different things.

**4. Price the instrument and require reconciliation on both axes.** Profiler overhead measured as a
same-binary on/off pair. Two reconciliations, each inside a boundary stated as a number before the
measurements are taken:
- *time* — stage totals plus unattributed time reconcile with `execute_ms`;
- *bytes* — per-domain stage totals plus unattributed bytes reconcile with the whole-query counter for
  that domain (the step-2 discipline applied to every cell, not just q21).

A profiler that perturbs the thing it measures, or whose parts do not sum on either axis, is not
evidence.

**5. Extend to the remaining candidates** — q18, q02, q08, q09 — only after the pilot passes 3 and 4.

**6. Select the first optimization target from attribution, not gap size.** q17 is the largest gap on
the board; that is not a reason to open q17. The target is the mechanism with the largest measured
attributable share across the set, which may be shared by several cells and may not be the worst one.

## Acceptance

- The contract exists as prose plus tests, defining the byte accounting domains as well as the stage
  timing, and every profile emitted by any path satisfies it.
- q17 and q20 emit profiles that reconcile on **both** axes inside the declared boundaries: stage
  totals plus unattributed time against `execute_ms`, and per-domain stage totals plus unattributed
  bytes against the whole-query counter for each domain.
- q21's 536-byte residual is resolved: named as unattributed bytes in a stated domain, or its two
  counters are declared non-summing with the reason.
- Profiler overhead is measured and stated; a run with profiling off is the timing of record.
- Every extended cell reconciles the same way on both axes, or is recorded as an honest red with the
  reason.
- No optimization mechanism is authorized by this ADR.

## Guardrails

- **No kernel, no policy change, no plan-shape change in this cycle.** It ends with an instrument and
  a ranked attribution, and the next ADR spends it.
- Stage vocabulary is not copied across lifecycle boundaries without an explicit comparability rule.
- Attribution is reported per-cell; a share averaged across queries with different shapes is not a
  finding.
- The sf=8 board stays council grounding, not a baseline: it was measured from a dirty tree
  (`cab8ea9f5952-dirty`). Any claim of movement against it needs a clean re-pin first.

## Risks

- **The contract may not span both classes.** Mitigated by making that a legitimate step-3 outcome
  rather than a failure.
- **Instrumentation may perturb spill-heavy cells** most, exactly where the gap is largest — step 4
  prices this before any number is banked.
- **The unattributed residual may dominate** on the non-chain paths, leaving the instrument technically
  correct and practically useless. If step 3 lands a residual — on either axis — larger than the
  largest named stage in that domain, the cycle re-opens step 1 instead of proceeding.

## Forward

The cycle produces a ranked attribution across seven cells, which selects the next optimization ADR.
Three things stay deliberately out of scope and are recorded so they are not lost:

- **Task multiplicity and memory attribution.** q18 peaks at 10.4 GiB RSS and q05 at 8.3 GiB against a
  2 GiB per-task coordinate. Per-task budgets do not bound process RSS across concurrent tasks, so this
  is not a proven breach — but nothing pins the mapping from per-task budget to process footprint. It
  is a grounding question for this cycle's step 1, and may become its own ADR.
- **q11 at sf=8.** Deferred, not blocked: the harness renders q11's HAVING fraction from an enumerated
  table of exact decimals listing only sf ∈ {0.01, 1, 10}. sf=8 is exactly representable
  (`0.0001 / 8 = 0.0000125`; oracle integer 800). Closing it makes the board 22/22.
- **The sf=10 publication board** on the ADR 356 coordinate, after the composed work lands.
