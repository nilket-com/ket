# ADR 521 - Outer count preaggregation on proven unique keys (reopens ADR 519)

ADR 519 closed red at its predeclared engagement gate: the transformation was sound, the
batteries were green at prototype level, and the official q13 correctly declined because no
uniqueness fact existed for `c_custkey`. That record stands unmodified — the red close was the
gate working. ADR 520 then made the missing fact exist without weakening anything: a
`VerifiedSnapshot` proof for `customer [c_custkey]`, bound to the artifact's schema fingerprint
and file hash, exposed only under full-hash loads, and reachable only through the proof
accessor. ADR 521 reopens the transformation as its own cycle with independent census and
cohort authority.

The theorem and economics are inherited from ADR 519 as immutable evidence, not re-derived:
the grouped-count-over-unique-key-left-outer identity (each output group is exactly one
preserved row's match set, or the coalesced null-extension), and the supported SQL twin at the
exact coordinate — 8,937.96 → 3,335.83 ms (+5,602.13 ms against the 3,554 ms floor), spill
5,131,808 → 0, RSS ~650 → ~344 MiB, join work → 76 ms, the surviving orders aggregation
~3.18 s. The twin also left the reopened cycle its sharpest constraint: it produced correct
rows and a matching schema fingerprint but a **different Arrow IPC digest**, so representation
identity is a rail the optimizer rewrite must meet and the twin was exempt from.

## Decision

1. The ADR 519 six-gate transformation, unchanged in shape: rebuild
   `Aggregate[k_L; COUNT(col_R)](L left_outer R)` as
   `Project[k_L, COALESCE(cnt, 0)](L left_outer Aggregate[k_R; cnt = COUNT(col_R)](R))` iff —
   LEFT OUTER with plain-equality ON and no residual/null-safe keys; grouping exactly by the
   preserved-side join key; the preserved key proven unique; exactly
   `COUNT(<non-null-counted right column>)`; deterministic right-side filtering retained below
   the preaggregate; original aggregate output metadata preserved.
2. **Hard addition — proof-only uniqueness.** The uniqueness gate resolves exclusively through
   the ADR 520 proof accessor (`EnforcedConstraint` or `VerifiedSnapshot`, ordered columns
   matching the join key exactly). `primary_key_columns` and estimated-NDV optimizer facts are
   not consulted and cannot satisfy the gate; a structural fixture pins each excluded path.
3. **Hard addition — explicit field-contract reproduction.** The rewritten aggregate output
   explicitly reproduces the original Arrow field name, type, and nullability; structural
   tests pin all three **before any execution test runs**. The coalesce projection is built
   from the original aggregate's field contract, never re-derived from the rewritten shape.
4. **Hard addition — census before cohort.** Census-wide digest and output-schema equality is
   a precondition for the q13 cohort: if any engaged cell's digest or schema fingerprint
   moves, the cycle stops there and reports — no cohort runs against a representation-broken
   rewrite.
5. Typed, latched control: `PlannerConfig::outer_count_preaggregation`, compiled default off
   at the seal; gauge env `TORON_TPCH_OUTER_COUNT_PREAGGREGATION`, loud "0"/"1" contract, row
   fields with provenance; plan-distinct cohorts; activation per ADR 472's rule.

## Acceptance

1. **Structural battery** (the 519 prototype's shapes as permanent tests): the q13 shape
   engages against a proof-bearing catalog — preaggregate below, outer join above, coalesce
   projection carrying the original field contract (name, type, nullability asserted at bind);
   declines pinned per gate — join kind, residual ON, null-safe keys, non-key/extra group
   keys, `COUNT(*)`/left-column/`SUM` aggregates, nondeterministic right filter — plus the
   proof-only pins: a `primary_key_columns`-only catalog declines, an estimated-NDV-only
   catalog declines, a proof with mismatched column order declines; knob-off byte-identity.
2. **Semantic battery**: unmatched preserved rows → 0, duplicate right matches, NULLs in the
   counted column, NULL join keys both sides, empty and all-filtered right sides; the engaged
   fixture asserts result equality and output schema/nullability identity between arms.
3. **Census, digest-gated**: q01-q22 + m00-m10 both arms at the exact coordinate from the
   sealed binary under the format-2 full-hash dataset; digests and output schema fingerprints
   identical everywhere — the q13 requirement the twin could not meet; fingerprint-changed
   cells run strict correctness in both arms; q19 sentinel. Required q13 evidence:
   `left_outer_grouped`'s ~8.9 s stage absent on-arm, spill 5,131,808 → 0, the orders
   preaggregation and collapsed join as the new envelope.
4. **Paired authority, gated on 3**: q13 cohort, 20 runs / 10 adjacent pairs, plan-distinct,
   fixed 3,554 ms floor, Hodges-Lehmann + 95% Wilcoxon from the persisted journal;
   regression-check cohorts for any other engaged cell; activation per ADR 472's rule; both
   verdicts reported either way.

## Guardrails

- Every gate fails closed; the knob-off arm is byte-identical.
- ADR 519's historical record is untouched — its plan, impl, and red receipt stand; this ADR
  references them as evidence and supersedes nothing.
- ADR 520's surfaces are consumed read-only: no changes to the proof model, verification,
  manifest, or loader; no new candidates are added in this cut.
- No executor changes; ADR 511-518 surfaces untouched.

## Risks

- The optimizer-built plan may price differently than the twin (the ~3.2 s orders
  preaggregation becomes the envelope either way); the cohort arbitrates against the
  unchanged floor.
- The field-contract rail is the known hard part; the census digest gate catches any residual
  representation divergence before measurement, and a digest break stops the cycle with a
  report, not a workaround.
- Engagement depends on the runtime dataset loading under full-hash verification; a
  measurement-only load would correctly expose no proof and decline — the coordinate already
  runs full-hash, and the census would surface any surprise.

## Forward

- Post-521, q13's residual is the orders preaggregation (~3.2 s class, ADR 507/508
  grouped-fold family; possible third `uncovered_generic_fold` tenant after q18 and q20).
- Wendelon's `EnforcedConstraint` production path remains out of scope, a named cross-ring
  follow-on.
- SUM/AVG/MIN/MAX variants of the identity stay out of scope until a cell demands them.
