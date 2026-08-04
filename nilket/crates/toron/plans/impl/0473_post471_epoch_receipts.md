# Post-471 epoch: q05 sf=8 bridge re-price (protocol run, no ADR)

The ADR 471 footer statistics moved q05 sf=8's plan (`757d7c62…` → `bedd2eeb…`,
digest unchanged), opening a measurement epoch. The 446 runtime-filter bridge was
re-priced at the same cell under the standing 449/451 paired protocol.

Cell: (q05, sf=8, memory_per_task 64 MiB, spill reserve 2 GiB), release, single
detached parent, 20 runs + 1 warmup, 21/21 journal, zero `BudgetExhausted`.

- Gate zero passed with identical reservoir identities to the prior epoch:
  OFF 47,989,007 rows, ON 7,692,229 (the ADR 461 filter-cap numbers).
- **HL OFF−ON 5,068 ms, 95% interval [4,788, 5,234], OFF median 26,753 ms,
  18.9% of baseline, material** — 1.89× the 2,675 ms floor.

The bridge's value survives better planning: same materiality verdict and the same
18.9% relative effect as ADR 461 measured under the old plan, reached from a
different plan and a different starting spill profile. Per standing rule the two
epochs' millisecond figures are not ranked against each other; what transfers is
the verdict and its scale, which is the question the re-price existed to answer.

Artifacts: `target/tpch/post471_q05_sf8_filter_cohort/`.
