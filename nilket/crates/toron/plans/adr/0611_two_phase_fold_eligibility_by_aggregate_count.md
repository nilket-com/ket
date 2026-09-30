# ADR 611 - two-phase fold eligibility by aggregate count

**Status:** PROPOSED. Successor to ADR 610, which retained
`GROUPED_FOLD_SHAPE = TwoPhase` on q01, with a median paired gain of
754.9 ms. Its q13 breadth pair fired the regression guard: a median
paired gain of −122.4 ms against a bar of 76.3 ms, with all five pairs
negative. This record tests one static eligibility rule, as one token:
**a batch folds two-phase only when its call list has at least two
typed aggregates.** It is a hypothesis. It projects no saving, and it
infers no board total.


## Context

**What ADR 610's diagnostics showed.** These are single runs per arm,
not a paired decomposition. The other stages were similar in those
single diagnostics.

| cell | typed aggregates | groups | `row_lookup_update` change |
|---|---|---|---|
| q01 | 8 | 4 | −753 ms |
| q13, dominant fold | 1 (`COUNT(x)`) | ≈1.2 M | +150 ms |

The candidate mechanism has two parts:
- **The saving.** Two-phase removes per-row, per-aggregate dispatch,
  which scales with the number of aggregates.
- **The cost.** It adds a second pass over each row's group state,
  scattered when the group set is large.

A single aggregate has little dispatch to save.

**Which cells the rule changes.** From ADR 610's gate receipts
(treatment arm), by instance:

| stays two-phase (≥2 typed aggregates) | falls back to row-at-a-time (0 or 1 typed aggregate) |
|---|---|
| q01 (8), q08 (2), q12 (2), q22 (2) | q03, q04, q05, q07, q09, q10, q11, q13 (both instances), q15 (both), q16 (the zero-aggregate instance), q17, q18, q20, q21 |

q02, q06, q14, and q19 are unchanged: they are row-at-a-time or have no
grouped fold. Only q01 and q13 have been paired. The other falling-back
cells took two-phase unmeasured under ADR 610. ADR 611 returns them to
the ADR 609 code path, and measures only q13 and q01.

A group-count threshold is the alternative. It needs a runtime reading
of group growth, and it is not this record.


## Decision

*The token.* `TwoPhaseEligibility { AllTyped, AtLeastTwoTyped }`, a
`const` read where ADR 610 decides `two_phase`:
- `AllTyped` is main's behaviour: every call typed.
- `AtLeastTwoTyped` also requires `aggr_expr.len() >= 2`.

Kernel selection, the phases, the receipt, and every semantic are ADR
610's, unchanged. A batch that does not qualify runs today's
row-at-a-time loop whole, as any ineligible batch does now. It builds
no template unless the receipt is armed, and then only for a typeable
call list, as in ADR 610. The token enum carries the same commented
`#[allow(dead_code)]` as the other `const` tokens.

*The receipt.* ADR 610's kernel receipt, with two added fields per
instance:
- `eligibility_rule`: the rule in force, `all_typed` or
  `at_least_two_typed`;
- `eligibility_reasons`: batches counted per reason. The reasons are:

| reason | meaning |
|---|---|
| `eligible` | typed kernels for every call, the rule satisfied: two-phase |
| `untyped_call_list` | some call has no typed kernel (a q02-type fallback): row-at-a-time under either rule |
| `below_aggregate_minimum` | typed for every call, but fewer than two calls, under `at_least_two_typed` only: row-at-a-time |

Under `all_typed`, a typed one-aggregate list is `eligible`. The shape
the batch ran under stays in `batches_two_phase` and
`batches_row_at_a_time`, as in ADR 610.

*The gate before any pair.* One run of each of the 22 queries per arm,
with the receipt, and digests checked against ADR 608's sealed cells.
It **fails closed** unless all of these hold:
- every digest and plan fingerprint matches on both arms;
- under `AtLeastTwoTyped`:
  - q13's two instances are row-at-a-time, with zero two-phase batches,
    and every batch's reason `below_aggregate_minimum`;
  - q02's instance reads `untyped_call_list` under both rules;
  - q01's instance is wholly two-phase, with ADR 610's exact eight
    kernels, each at `rows_folded`, and zero per-row updates;
- every cell's instance shapes match the table above. Any difference is
  named, and fails the gate for correction of the table.

*The pairs.* ADR 597's driver, ADR 608's sealed manifest, ADR
592/593/595/599 as in ADR 610, one attempt each:
1. **q13, recovery.** Control `AllTyped`, treatment `AtLeastTwoTyped`.
   ADR 588's rule **decides the token**: retained only if the median
   paired gain exceeds `2 × range_control` and every pair is strictly
   positive. Otherwise the closing commit reverts the token to
   `AllTyped`.
2. **q01, non-regression.** Both arms run q01's identical two-phase
   path, so the pair checks that the rule's added test costs nothing
   measurable. It uses ADR 607's regression guard. A regression means
   the treatment slower by more than `2 × range_control`, with every
   pair negative, or a digest mismatch, or treatment-only spill. A
   regression opens a new record and is never a silent revert. Any
   other outcome is non-regression. ADR 610 remains the evidence for
   q01's gain over row-at-a-time.

Peak RSS, with its label, and spill are reported beside every verdict.

*Mechanism, after the verdicts.* Single diagnostic runs of q13 per arm
with node attribution. They report `row_lookup_update` and its sibling
stages, with the receipt. These are not a paired decomposition.


## Acceptance

- Fixtures:
  - eligibility under both forms for call lists of 0, 1, 2, and 8 typed
    aggregates, and for an untypeable list;
  - the receipt's `eligibility_rule`, and each reason:
    - `eligible` under both rules;
    - `untyped_call_list` under both rules;
    - `below_aggregate_minimum` under `at_least_two_typed` only;
    - a typed one-aggregate list under `all_typed`, recorded as
      `eligible`;
  - fold parity between the forms on a one-aggregate and a
    two-aggregate call list.
- The token asserted by diff: one insertion, one deletion.
- Ladders green on both trees on bigfoot, fmt clean, and release builds
  warning-free under both token settings.
- The gate's receipts, or its refusal. The q13 pair and verdict. The
  q01 pair and interpretation. The q13 diagnostics.
- Plan commit, then one implementation, results, and closure commit.


## Guardrails

- Only the eligibility decision changes. The kernels, the lookup paths,
  the semantics, and the output are ADR 610's.
- Cells that fall back return to the current row-at-a-time code path,
  with ADR 609's borrowed lookup. Most of them have no same-tree paired
  latency evidence for either shape. ADR 608's board predates ADR 609,
  and ADRs 609 and 610 paired only q01, q09, and q13. So the board
  re-seal in Forward is necessary, and the closing record lists the
  unpaired fallback cells prominently.
- ADR 588's rule, the twelve-row contract, and ADR 599's gate,
  verbatim. One attempt per pair.


## Risks

- Aggregate count is a proxy. A one-aggregate fold over few groups may
  lose a small two-phase gain, and a many-aggregate fold over very many
  groups may still regress. Neither case was measured here. The gate
  names the cells, and later records can refine the rule.
- If q13's pair does not establish recovery, the token reverts under
  the rule. The regression's cause then remains unresolved: paired
  noise or other costs may obscure the aggregate-count mechanism
  without disproving it. ADR 610's regression stays open for another
  record.


## Forward

- If retained: a board re-seal on main, measuring the campaign total
  after ADRs 609–611.
- A columnar accumulator layout, and q01's argument evaluation (about
  0.9 s of Decimal128 arithmetic), remain candidates.
