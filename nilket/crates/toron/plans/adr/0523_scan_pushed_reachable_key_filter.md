# ADR 523 - Scan-pushed reachable-key filter (q05)

ADR 522 closed red with a precise structural lesson: a branch-semi must re-traverse the 48M
lineitem rows the suffix build already pays for, so its net (2,415-3,565 ms) cannot clear the
floor. The dataset twin banked the gross: +4,524.5 ms at the exact coordinate with the chain
shape preserved. Reorientation is parked — its gross owner is the 2.86 s build prep, already
below the floor, and it would trade 1.82M probes for 48M probes while preserving scan and
exchange; no credible floor-clearing path.

The remaining route fuses the reduction into the traversal that already happens: a
**scan-pushed membership filter** — the 16,042 ASIA supplier keys tested against `l_suppkey`
during the lineitem scan's existing decode, before any suffix build or materialization. The
arithmetic gate, predeclared from ADR 522's ledger:

| component | value |
|---|---:|
| dataset-twin gross ceiling | 4,524.5 ms |
| scan saving the twin got that a pushed filter does not (fewer file bytes) | ~510 ms |
| remaining ceiling | ~4,014.5 ms |
| fixed materiality floor | 3,554 ms |
| **allowed total filter + acquisition overhead** | **~460.5 ms** |

## Phase 0 - benchmark the filter cost before any mechanism code

The allowance is tight enough that the filter's real cost decides the ADR before design does.
Phase 0 measures the engine's own probe machinery at the exact volume and coordinate:

1. A temporary ladder probe rebinds m06 to `lineitem INNER JOIN supplier ON l_suppkey =
   s_suppkey` — 48M probes against an 80,000-key cache-resident build in which every row
   matches: pure probe-path cost, no reduction effects. m08 (`COUNT(l_orderkey)`, 265.4 ms)
   prices the decode floor; the l_suppkey decode adds one m08-class column.
2. Marginal probe cost = patched-m06 median − m08 median − one Int64 decode. This
   **overstates** the mechanism twice: the real build is 16,042 keys (5x smaller, deeper in
   cache), and a fused scan filter skips the join-node scaffolding the probe pays. It is
   therefore a fair upper bound and a generous pass condition.
3. Five interleaved runs; the probe patch is removed with a targeted edit afterward.

**Predeclared decision: if the marginal probe cost exceeds ~460.5 ms, ADR 523 closes red
immediately and the board returns to q09.** If it fits, the mechanism proceeds under the hard
requirements below.

## Decision (contingent on Phase 0)

1. The filter reuses **already-prepared supplier-domain state** — the chain's own
   supplier/nation/region runtime inputs derive the 16,042-key set once; no cloned joins, no
   second acquisition.
2. **One scan traversal**: the membership test rides the existing lineitem scan decode as a
   row filter; no added pass, no added materialization.
3. **Chain recognition preserved**: the reoriented-chain route, keys, intents, exchanges, and
   schemas are unchanged; the ADR 522 SQL twin proved shape loss costs more than the
   mechanism gains, so recognition identity is a structural gate, not a preference.
4. **No false negatives, null-safe**: a row passes unless its key is provably outside the
   set; NULL keys pass through to the join's own semantics (which eliminate them exactly as
   today). The filter may only remove rows the downstream inner joins would remove.
5. Typed, latched control: `PlannerConfig::scan_pushed_key_filter`, compiled default off at
   the seal; gauge env `TORON_TPCH_SCAN_PUSHED_KEY_FILTER`, loud "0"/"1" contract, row fields
   with provenance; plan-distinct cohorts; activation per ADR 472's rule.

## Acceptance

1. Phase 0 as above, with immediate-close authority.
2. Structural battery: q05 engages with the filter on the scan and the chain route intact
   (fingerprint changes, route receipt unchanged); declines pinned (no deterministic
   filtered source subtree, non-equality edge, outer/anti context); knob-off byte-identity;
   single derivation of the key set (no cloned subtree in the bound plan).
3. Semantic battery: NULL branch keys pass through identically; duplicate keys; empty and
   all-filtered sources; false-negative fixture (a key wrongly absent from the set must be
   impossible by construction — the set is derived from the same inputs the join consumes).
4. Census: q01-q22 + m00-m10 both arms, digests identical; q05 evidence — suffix build
   47,989,007 → ~9,623,900, suffix-1 emissions 7,283,512 → ~1,460,247; fingerprint-changed
   cells run strict correctness both arms; q19 sentinel; engaged cells beyond q05 get
   regression cohorts.
5. Paired authority: q05 cohort, 20 runs / 10 adjacent pairs, plan-distinct, floor 3,554 ms,
   Hodges-Lehmann + 95% Wilcoxon; both-way verdicts.

## Guardrails

- Fail closed everywhere; knob-off byte-identical; no executor contract changes beyond the
  scan's existing pushed-predicate surface.
- ADR 511-521 surfaces untouched; the ADR 520 proof surface is not involved (this is a
  row-filter, not a uniqueness argument).

## Risks

- The allowance may simply not fit — the predeclared red path, costing one ladder probe.
- Deriving the key set from prepared state imposes an ordering dependency (supplier-domain
  inputs before the lineitem scan); if the runtime cannot honor it without restructuring,
  that is a decline, not a redesign.
- Scan-level filtering interacts with PBC pushdown receipts; census-adjudicated.

## Forward

- On red: return to q09 (probe-side volume and the partsupp generic gap, 6.5 s board gap).
- On activation: q05's residual re-ranks (remaining exchange and probe shares); reorientation
  stays parked unless the reduced shape re-opens it.
