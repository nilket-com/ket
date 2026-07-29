# ADR 516 - Suffix-probe attribution: split the shared join-probe envelope

q08 and q09 are the board's remaining ranked cells, both on the spill-free
`reoriented_inner_chain_grouped` route, and both owned by one stage family: `suffix_probe` costs
3,834.7 ms of q08's 7,762.9 ms handler and 6,082.4 ms of q09's 12,663.7 ms — three q09 suffix
joins at 4,028.7 / 1,781.7 / 272.0 ms under identical call structure (23,436 calls = 4
partitions x 5,859 base batches). The leaf lumps exactly what a remedy would need separated:
probe lookup, match materialization, and payload read-back, with no row counters — so
amplification is inseparable from per-row cost, and the striking per-suffix asymmetry at equal
call counts (a 12M-key orders build versus a 25-row nation build) is a hypothesis, not a
finding. Host CPU sampling is unavailable (`perf_event_paranoid = 4`) and stays unavailable:
this ADR does not relax the sysctl.

This is an attribution-only cycle, ADR 508's discipline applied one level deeper. **No remedy is
selected, sketched, or implemented here**; the deliverable is independently ranked premises for
q08 and for each q09 suffix join, and the successor remedy must follow that split.

## Decision — the instrumentation contract

1. **Time the boundaries the code actually has.** The Int32/Int64 probe driver interleaves lookup
   and match expansion in existing windows of at most 64 keys; Arrow output gathering is already a
   separate chunk phase. Time those windows and chunks, never individual rows, accumulate
   `Duration`s locally, and publish once per suffix call. Generic-key probing is outside this first
   instrument because it has no window boundary; both measured cells use the Int64 path. Anything
   requiring per-row clocks is out of contract (the ADR 510 lesson: no perturbing per-row
   estimator).
2. **Exact parent reconciliation.** `suffix_probe` becomes the parent of three predeclared
   refinements — `suffix_lookup`, `suffix_match_expand`, `suffix_payload_take` — plus an explicit
   residual. The first is table lookup, the second expands returned row lists into the join
   selection, and the third builds Arrow output through `take`. Children + residual must sum to the
   replay-scoped parent, and a failed reconciliation fails the receipt, not the query.
3. **Exact flow counters per suffix runtime**: input probe rows, non-null keys looked up, lookup
   windows, matched references, emitted rows, output column-values, and output array bytes.
   Multiplicity is matched references per input probe row and per non-null key. The bytes are the
   materialized output arrays' owned memory, not a claim about source-memory traffic; exact payload
   bytes read from caches are unavailable without hardware counters. Counters are checked sums, not
   samples.
4. **Stable suffix identity.** Every stage row carries the suffix runtime's replay identity so
   q09's three builds can never be merged or confused by call position; the receipt reports each
   suffix join as its own premise.
5. **Locality proxies, honestly bounded**: each suffix runtime's build representation, build-batch
   bytes, index-accounted bytes, distinct-key count, entry and bucket capacities, load numerator /
   denominator, and key width ride the receipt as stated facts. The receipt's vocabulary is
   "consistent with lookup-locality cost" at most — no hardware cache-miss claims, ever, without
   counters this host cannot provide. Hardware counters remain optional future evidence, not a
   dependency.
6. **Perturbation gate.** Interleaved profiler-off/profiler-on runs (adjacent ordering, equal
   counts) quantify instrumentation movement per cell; the predeclared bound is stated in the
   impl plan, and evidence from any run set exceeding it is rejected rather than corrected.

The instrument activates only under the existing node-attribution profile path. The disabled path
opens no clocks, allocates no observer, and takes no ledger lock beyond the pre-existing
node-attribution checks; join selection and output materialization keep the same code and ordering in
both states. There is no planner or executor mechanism change, no knob, no census impact, and no
performance cohort — attribution receipts are the whole deliverable.

## Acceptance

- The reoriented-chain vocabulary gains the three children and residual with reconciliation
  receipts on both cells; existing consumers of the vocabulary (q08/q09 receipts, the 508
  baseline artifacts) remain readable.
- Unit tests pin: child refinements sum within the matching replay-scoped parent; counters are exact
  on fixtures with known multiplicities (including a duplicate-heavy build and an empty-match
  batch); window/tail accounting is exact; per-suffix identity survives reordering; generic keys do
  not publish a falsely complete split; profiler-off emits nothing and changes no digest.
- The attribution runs: interleaved off/on at the exact coordinate, perturbation quantified and
  within bound; then per-cell receipts producing the ranked premise table — q08's suffix join
  and each of q09's three, each with time split, flow counters, multiplicity, output-array volume,
  and locality proxies.
- The receipt ends with the ranked premises and explicitly **no remedy selection**.

## Guardrails

- No per-row sampling; no sysctl changes; no hardware-counter dependency.
- No behavior change anywhere: profiler-off paths are byte-identical, and the census (run once,
  both profiler states) must show identical fingerprints, digests, and route facts.
- The 3,554 ms floor is not consulted — nothing here is a performance claim.

## Risks

- The three-way split may itself carry overhead inside tight loops even batch-scoped; the
  perturbation gate exists to catch exactly that, and a rejected run set closes the ADR red on
  instrumentation cost rather than shipping perturbed numbers.
- The split may show the envelope is dominated by the residual (work between the three named
  phases); that is a valid attribution outcome and shapes the successor differently.

## Forward

- The successor remedy ADR follows the ranked split: whichever of lookup, match expansion,
  payload take, or residual dominates — per suffix join — defines its mechanism family.
- q18's two sub-floor folds stay parked unless one mechanism covers both; q21's cache residual
  stays parked per the standing decision.
