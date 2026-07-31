# ADR 524 - Partsupp composite-key lifecycle attribution (q09)

The fresh q09 grounding at `8e927b92` (8.842 s span, fully attributed) leaves three owners,
each individually below the 3,554 ms floor: the opaque partsupp suffix (replay 2, 1,585 ms
parent envelope, no sub-stages — the composite generic key is outside the ADR 516 sub-stage
contract), `runtime_prep` (1,639 ms across four builds, unsplit), and the 48M suffix-1 probe
(873 ms, already split: lookup 568 / amplification 201). The flow counters license one damning
arithmetic fact: partsupp costs **607 ns per probe** where the orders suffix — same 2,610,723
probes, a build twice as large — costs 37 ns: the 16-byte composite path is ~16x per probe.

But a remedy aimed at that ratio would today be priced against an envelope stitched together
by inference: the partsupp share of prep is unknown inside 1,639 ms, the partsupp share of
cleanup unknown inside 690 ms, and the 1,585 ms parent hides its lookup/materialization split.
Per the standing doctrine — an envelope share bounds where cost sits, not what it is — this
cycle is **attribution only**: close the partsupp lifecycle boundary so the successor prices
one coherent removable envelope instead of summing unrelated sub-floor pieces.

## Decision

Extend the ADR 516 instrument on the reoriented-chain vocabulary, profiler-only, no engine
behavior change:

1. **`runtime_prep` per replay/build**: predeclared per-build prep sub-stages (one per suffix
   replay identity) with rows and retained bytes, reconciling to the `runtime_prep` parent
   with an explicit residual — reconciliation-or-fail, the 516 contract. Each child begins
   before that stage's scan-build decision/admission work and ends only after its runtime is
   constructed; decision pre-scans cannot leak into an unattributed parent residual.
2. **Replay-2 sub-stage coverage**: extend `suffix_lookup`, `suffix_match_expand`, and
   `suffix_payload_take` to the generic composite-key suffix path, batch-scoped tokens only
   (never per-row), with the same parent reconciliation and flow counters the specialized
   path carries.
3. **Cleanup ownership per build**: per-replay cleanup sub-stages reconciling to the
   `cleanup` parent with explicit residual.
4. **Composite-key representation facts, stated not interpreted** (the 516 locality-proxy
   discipline — no hardware-miss claims): key width in bytes, table capacity and load factor,
   retained bytes, and the hash/index type name for each build, generic and specialized
   alike. Fact collection is O(1): exact fixed-width key/retained bytes are reported only
   where the representation proves them; variable-width or otherwise untracked ownership is
   `unknown` with a reason. The profiler never walks entries to manufacture a fact.

Construction is profiler-gated and computed once per runtime (the ADR 516 corrective-seal
lesson: no per-call shape walks); with the profiler env unset the instrument is absent and
the engine byte-identical.

## The deliverable and its predeclared decision

One coherent maximum removable envelope for the partsupp build lifecycle:

```text
partsupp prep + replay-2 probe (all sub-stages + residual) + partsupp cleanup
```

- If the lifecycle envelope does **not** clear 3,554 ms: **q09 is parked**, recorded with the
  full split — no summing of unrelated sub-floor stages to manufacture materiality.
- If it clears: the successor may legitimately target a compact composite-key build/probe
  lifecycle as one mechanism owning one envelope; remedy selection stays out of this ADR.

## Acceptance

1. **Perturbation gate, verbatim ADR 516**: five interleaved profiler-off/on q09 pairs at the
   exact coordinate; median perturbation bound 3%; a breach is rejected, not corrected; two
   consecutive rejections close the ADR red.
2. **Reconciliation**: every new parent/child family reconciles or the profile fails —
   `runtime_prep`, replay-2 `suffix_probe`, and `cleanup` each report attributed + residual;
   the q09 profile reports `coverage_complete=true` (the current honest `false` for replay 2
   is the gap being closed).
3. **Counter discipline**: flow counters from complete artifacts; counter domain and cadence
   stated on every new stage; digests, fingerprints, and route facts identical with the
   profiler on and off.
4. **Both cells sanity**: q08 (same vocabulary, no generic suffix) profiles unchanged except
   the new prep/cleanup splits; its perturbation spot-pair stays within the bound.
5. Structural tests pin: profiler-off byte-identity, once-per-runtime construction (the
   regression class from 092e574a), reconciliation failure on a synthetically missing child.

## Guardrails

- No planner or executor behavior change; no knob beyond the existing profiler env; census
  not applicable (plans cannot change — asserted by fingerprint identity in the gate pairs).
- Timers batch-scoped only; representation facts are recorded fields, never inferences.
- Per-build cleanup is order-sensitive lifecycle attribution and therefore an upper bound on
  removable cost, not a claim that changing one build recovers its measured teardown wall.
- ADR 511-523 surfaces untouched.

## Risks

- The generic-path token insertion may perturb the hot loop; the perturbation gate is the
  authority and closes red rather than shipping a lying instrument.
- The lifecycle envelope may land below the floor — that outcome parks q09 honestly and is
  priced in advance.

## Forward

- On a clearing envelope: a successor compact composite-key lifecycle ADR (build layout +
  probe path + teardown as one mechanism), priced against this attribution.
- On parking: the board's next tier is q18's shared-fold family (three tenants) and the
  small-absolute ratio tail; q21 and q05 stay parked.
