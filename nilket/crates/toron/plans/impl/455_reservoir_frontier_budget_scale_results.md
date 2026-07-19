# ADR 455 implementation receipt

The predeclared crossover prediction matched **exactly at both probed cells** — and the
probe demonstrated that the frontier is not two-dimensional: the spill reserve is a
third coordinate, proven by walling the first 128 MiB bracket before the reservoir
threshold could speak. The result is therefore a conditional two-point confirmation,
not a standalone law.

## The observed cells (q05, release, strict parquet, 450 taxonomy)

Prediction under test: retained-cap crossover s\*(B) ≈ 4 × (B / 64 MiB).

| memory_per_task | spill reserve (provenance) | predicted s\* | probed rungs | observed | reservoir rows at commit |
|---|---|---|---|---|---|
| 64 MiB (450/454) | 512 MiB (default) | 4 | — (prior) | committed at sf=4 | 3,885,538 |
| 96 MiB | 512 MiB (default) | 6 | 5, 6, 7 | sf=5 no, **sf=6 committed** | 7,057,952 |
| 128 MiB | 512 MiB (default) | 7–9 walled | 7 | `BudgetExhausted(SpillBytes)` at sf=7 | — |
| 128 MiB | 2 GiB (explicit) | 8 | 7, 8, 9 | sf=7 no, **sf=8 committed** | 11,742,021 |

Verdict per Rail 3, scoped to what the evidence supports: **the retained-cap crossover
follows the predicted linear relation at the two observed cells, conditional on a
non-binding spill reserve.** Sub-frontier rungs decline via Guard A (resident path) in
every case, the same non-commit mode 450 saw.

What this does *not* establish: a universal budget×scale placement rule. The spill
reserve is a demonstrated third coordinate — 96 MiB reached its crossover under the
default reserve while 128 MiB required 2 GiB — so a future frontier or paired-ablation
cell must be stated as **(memory_per_task, spill_reserve, scale)** with the reserve
predeclared non-binding, and any extrapolation (including 256 MiB → s\* ≈ 16, which is
also beyond the gauge's sf=10 bound) is a hypothesis to bracket, not a placement.

## Rail 2b — the spill-reserve confound (found, fixed, evidenced)

The first 128 MiB bracket hard-errored at sf=7:
`BudgetExhausted(SpillBytes, requested 536,889,440, available 536,870,912)` — the
512 MiB `spill_bytes_reserved_per_query` test default exhausted ~18 KiB short of its
ceiling before the reservoir threshold was reached. Varying `memory_per_task` alone
had left a second budget dimension implicit. The gauge now exposes
`TORON_TPCH_SPILL_BYTES_RESERVED` (positive bytes, loud rejection, provenance
`default`/`explicit`), recorded alongside the task budget in every child row, summary,
and ladder record. The walled attempt is preserved at
`target/tpch/adr455_frontier/b128_spillwalled/`; the confirmed bracket re-ran with the
reserve explicitly at 2 GiB, making the reservoir threshold the only variable under
test.

## Harness deltas

- `gauge_spill_bytes_reserved` beside `gauge_memory_per_task`; both applied in
  `tpch_execution_context` when explicit; defaults unchanged.
- Ladder rung/terminal records carry both budget dimensions with provenance
  (`adr455_ladder_records_carry_budget_provenance`); the env contract for both knobs is
  pinned in `adr454_gauge_budget_defaults_and_overrides_with_provenance`.

Wall clocks above are single profiled children — context only, comparable to nothing
outside this probe.
