# single-query-task board epoch close at 53f18ebc

The optimization epoch that ran ADRs 511-526 under the fixed 3,554 ms per-cell materiality
floor is declared **converged** at `53f18ebc`. The floor stays frozen — lowering it now would
turn a successful stopping rule into a moving target. This record preserves the era's final
state; the receipts live in the ADR chain. “Single query task” is the exact Toron coordinate:
the harness invokes one local `Executor` with one `TaskBudget`. It is not a retrospective claim
that every instruction ran on one OS thread. The rows did not persist the route-local
`TORON_PARTITION_PARALLELISM` setting, and the partitioned spilled scan-build path may dispatch
bounded blocking work internally. DuckDB's reference arm alone was explicitly pinned to one
thread as a session fact.

## What the era delivered

Activated, default-on: ADR 511 (filtered build-side swap), 512 (reachable-key reduction),
514 (compact build sizing), 515 (membership-first composition), 517 (side-local filter
routing), 518 (semi-filtered reachable keys), 521 (outer count preaggregation on proven keys,
via the 519 red → 520 uniqueness-evidence infrastructure → 521 reopening arc). Board effect:
roughly 8x one-thread DuckDB before ADR 511, ~6.4x after 512, **4.2x** at the canonical
post-521 snapshot (`521_three_engine_board_post_activation_results.md`), with q17
35.5 → 4.2 s, q18 21 → 7.5 s, q21 16.3 → 12.5 s, q08 7.9 → 3.9 s, q09 12.9 → 8.6 s,
q20 8.7 → 2.0 s, q13 8.5 → 3.1 s and spill eliminated everywhere except q21's 27 MB.

Closed red with receipts, each at Phase 0 or gate cost only: 513 (estimate-based route flip),
516-era instrument corrections, 519 (uniqueness unprovable — the gate working), 522 (branch
semi: the 48M-row re-traversal), 523 (scan-pushed filter: 778 ms probe vs 460.5 ms
allowance), 524 (instrument validity), 526 (best-effort compact lifecycle 824.8 ms vs
579.5 ms budget). The 525 attribution priced q09's partsupp lifecycle with authority
(E = 4,133.5 ms) and 526 proved the margin unreachable — parked with the bound recorded.

## Terminal board state

Every named opportunity is below the frozen floor per cell: q21 parked (cache-bound residual,
no narrower mechanism), q05 parked (the reachable-key family closed at 522/523 — any
48M-touch mechanism pays 0.8-1.5 s against a 0.46 s allowance), q09 parked (524-526 lifecycle
arc), and the shared-fold family closed as **false** (`shared_fold_family_grounding_post526_results.md`
— q18's folds share no kernel; the outer envelope is materialization wearing a fold label;
q20's tenancy vacant at 14 ms). Sub-floor observations are recorded in their groundings, none
authorized.

## Doctrine state at close

Cascades is not triggered: one shape-exposure shim (ADR 518), zero conditional-rewrite
backlog, no fact-blocked material wins outstanding, and no evidence that failure to retain
and cost alternatives blocks anything material. The Wendelon declared-constraint tier remains
worthwhile architecture work but is not the next performance experiment. The next council
topic is the **attribution-only 1/2/4/8-thread scaling atlas** (its own plan), whose verdict
tree — good scaling / uneven-completion / balanced-but-inefficient / thread-dependent plans —
decides whether morsel scheduling ever becomes evidence-backed.
