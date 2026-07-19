# ADR 459 implementation receipt

The ADR 446 "12x" spill-scope gap is **sealed closed** at its home coordinates: at
sf=10, on the post-458 format, whole-query task spill equals the grouped-chain root
delta **exactly, to the byte, in both arms**. There is no out-of-root spill producer to
attribute; Rail 3 never ran. The arc's oldest queue item retires with this receipt.

## The probe (Rail 1: one profiled child per arm, release, strict parquet)

Coordinates: (64 MiB task explicit, 4 GiB reserve explicit, sf=10), fresh spill
scratch and profile dir per arm, no `BudgetExhausted` (the predeclared 8 GiB raise was
not needed).

| arm | whole-query `spill_bytes` | root `task_spill_delta` | remainder | reservoir bytes | reservoir rows |
|---|---:|---:|---:|---:|---:|
| ON | 335,244,304 | 335,244,304 | **0** | 251,574,832 | 17,893,398 |
| OFF | 921,747,216 | 921,747,216 | **0** | 838,077,744 | 59,986,052 |

Identity checks pass by the scale-parametric oracle: OFF reservoir rows equal the
sf=10 strict lineitem count (59,986,052 — the very number 446 recorded), ON strictly
less. Both children `status: pass`.

Alongside the sf=8 evidence already in hand (458's gate-zero profiles + cohort rows:
whole == root exactly, ON 231,643,984 / OFF 735,589,072), the measured picture is:
**q05's spill is entirely inside the grouped-chain root** — reservoir plus runtime
build — at every measured cell of the post-458 format.

## What closes, and how far the claim reaches

The 446 observation (~5.35 GB whole-query vs ~279 MB root at sf=10) is explained by
its era: the pre-448 scope confusion (whole-query totals compared against root-scope
deltas as if commensurable) compounded by the pre-458 page format (per-fragment pages
inflating spill volume by an arm-dependent factor). Those measurements are not
re-judged — they were true of that format and those scopes. What retires is the open
question: on the current format, with typed scopes, there is no unattributed spill in
any measured configuration (sf=8 cell, both arms; sf=10, both arms, 64 MiB/4 GiB).

Scope of the claim, per the plan's guardrail: closure is established for the measured
configurations, not asserted as a universal invariant; `spill_roles_reconciled` and
the `spill_io` inventory remain the standing tripwires if a future cell disagrees.

No wall or materiality claims; wall clocks from the probe are context in the raw
artifacts (`target/tpch/adr459_gap_probe/`) and compare to nothing.
