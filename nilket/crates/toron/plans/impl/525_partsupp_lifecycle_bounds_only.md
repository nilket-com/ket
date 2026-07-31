# impl 525 - partsupp lifecycle bounds plan

The minimum sufficient attribution cut: per-replay prep and cleanup clocks around existing
lifecycle regions, one fail-closed partition rule for the reconciler, E computed from ADR
516's untouched replay-2 parent plus the two new shares. Generic probing is not touched in
any way. Two strikes — perturbation or reconciliation, either cell — park q09 attribution.

## Immutable inputs

- tree: post-524-red main (`ffde1074` lineage; executable source byte-identical to
  post-523), all activated knobs default on;
- exact coordinate: strict sf=8 persisted Parquet (format 2, full-hash), budgets 2 GiB /
  2 GiB explicit; fixed floor 3,554 ms;
- fresh grounding (at `8e927b92`): replay-2 parent 1,585 ms (reconciled); runtime_prep
  1,639 ms and cleanup 690 ms, both invocation-scoped and unsplit; builds part 87,125 /
  partsupp 6,400,000 / orders 12,000,000 / nation 25; E interval [1,585, 3,914];
- ADR 524's red receipt (`ffde1074`), binding on this cut: the generic probe's execution
  order must not change (no windowing, no staging, no per-row timers), and replay-scoped
  children under an invocation-scoped parent need an explicit partition rule;
- ADR 516 contracts: batch-scoped tokens, reconciliation-or-fail, perturbation gate at
  absolute 3% median over five interleaved pairs; 092e574a once-per-runtime construction.

## Step 1 - clocks and the partition rule

In exec/join/streaming.rs, profiler-gated:

- one `prep_build` child per suffix replay identity bracketing that build's existing
  preparation region (no code motion), recording elapsed, rows, retained bytes;
- one `cleanup_build` child per replay bracketing its existing teardown region likewise;
- the node-attribution reconciler learns one fail-closed rule: replay-scoped children
  partition an invocation-scoped parent iff replay IDs are unique, required coverage is
  complete, summed child time ≤ parent, and exactly one explicit global residual remains;
  any violation fails the profile with a named verdict.

No other stage, counter, or code path changes; the generic probe path is untouched.

## Step 2 - tests and seal

Structural: profiler-off byte-identity; once-per-runtime construction; partition-rule
batteries (duplicate replay ID, missing replay, child sum exceeding parent, absent residual —
each fails closed); q08 profiles gain only the new prep/cleanup children. Full battery with
direct exit statuses; seal `impl: adr 525 partsupp lifecycle bounds`, push, release build.

## Step 3 - gates and the envelope

From the sealed release binary at the exact coordinate:

- five interleaved profiler-off/on pairs for **q09** and five for **q08**; absolute median
  bound 3% per cell; identity (fingerprint, digest, rows, route, spill) required per pair;
- q08 must retain complete stage coverage; q09 may retain only ADR 516's three known,
  unsupported replay-2 generic leaves (`suffix_lookup`, `suffix_match_expand`,
  `suffix_payload_take`). They do not count as an ADR 525 rejection because the reconciled
  replay-2 `suffix_probe` parent is E's declared input. Any wider missing set, failed existing
  parent, or failed new lifecycle partition does count;
- **two strikes anywhere — perturbation or reconciliation, either cell — park q09
  attribution; the board moves to the shared-fold family with no third attempt;**
- on clean gates, one profiled q09 run supplies
  `E = prep_build(partsupp) + suffix_probe(replay 2) + cleanup_build(partsupp)`.

## Step 4 - receipt and the predeclared decision

`perf:` receipt with the per-build prep/cleanup table, the partition-rule verdicts, both
cells' gate tables, and E against 3,554 ms: below → q09 parked with the split recorded;
at-or-above → the compact composite-key lifecycle is the named successor target. No remedy
selected either way.

## Repository gates

The standard council seal and changed-surface blocks via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.
