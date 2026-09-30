# ADR 607 - q09 coverage of the cap check's batch reuse

**Status:** PROPOSED. Successor to ADR 606. ADR 606 retained
`WRAPPED_CAP_CHECK_BATCHES = Reuse` on q05. q05 went from 5,170.6 ms to
4,735.0 ms, a median paired gain of 444.7 ms, and peak RSS rose
1.14 GiB. The token is global: it applies on every query that reaches
the reoriented chain's wrapped arm. This record measures its effect and
cost on q09, the other cell where ADR 605 found doubled build-side
scans. **It is coverage of a retained token, not a second retention
decision.** No code changes. It projects no q09 saving and no new
22-cell total.


## Context

**q09 before ADR 606.** ADR 605's census of q09 (executable `2560c00c…`,
pre-606) shows route `reoriented_inner_chain_grouped` and plan
fingerprint `5b3e374cd2d74c4a`. Four build-side tables are scanned
twice in immediate succession, with identical rows and batches:

| table | rows per scan | decoded bytes per scan | batches per scan |
|---|---|---|---|
| part | 1,600,000 | 87,146,202 | 196 |
| partsupp | 6,400,000 | 205,123,520 | 782 |
| orders | 12,000,000 | 144,296,640 | 1,465 |
| nation | 25 | 776 | 1 |

The second scans add 20,000,025 rows and 436,567,138 decoded bytes.
Supplier (80,000 rows) is scanned once, and lineitem (47,989,007 rows)
is the probe, scanned once. On the sealed anchor (ADR 603), q09 is
Toron 7,114.0 ms against DuckDB 1,635.0 ms, a gap of 5,479 ms.

**The build shapes, from the sealed plan** (ADR 604's census,
`adr604-census/q09/structural_census.json`, compact shape):

| table | build input | ADR 606 path | under `Reuse` |
|---|---|---|---|
| part | `Exchange(parts=4, Filter(EvaluateInToron, p_name LIKE "%green%", Scan(part)))` | `streamed_filter_exchange_attempt` | control, not reused |
| partsupp | `Exchange(parts=4, Scan(partsupp))` | `bound_exchange` | reusable if `MaterializedBatches` |
| orders | `Exchange(parts=4, Scan(orders))` | `bound_exchange` | reusable if `MaterializedBatches` |
| nation | `Exchange(parts=4, Scan(nation))` | `bound_exchange` | reusable if `MaterializedBatches` |

ADR 606 leaves the streamed filter-exchange path on control by design,
since the treatment does not reproduce it. So part stays scanned twice
under `Reuse`. Supplier is a bare scan inside the lineitem join, not a
wrapped build stage, which fits its single scan. The duplicated volume
this record can reach is partsupp, orders, and nation: 18,400,025 rows
and 349,420,936 decoded bytes. Part's 1,600,000 rows and 87,146,202
bytes remain.

**What is not known.**
- The decision on each of the three reusable stages. The path follows
  from the plan shape, but the decision (`MaterializedBatches`, `Stream`,
  or another) depends on the cap check at run time, and q09's stages
  were never receipted.
- The timing effect. The reachable duplicated volume is smaller than
  q05's (18.4 M rows against 48.1 M). That does not determine the
  timing effect in either direction. This record projects no saving.


## Decision

*The arms.* ADR 606's two executables, unchanged, already staged and
hash-verified on strix:
- control: `artifact/adr606-control`, tree `2e0c2aab`, executable
  `bd72eec2c373434aefdd91b4f945dfd92f390c11f43fac73643b28748c3b77a7`,
  `Discard`;
- treatment: `artifact/adr606-treatment`, tree `64a8100b`, executable
  `889fa4891020c53a59bd5a2bcb26273caf3d5f180a49818aecaa8f0248e8a2d8`,
  `Reuse`.

They differ by the one token. The treatment's code is main's code: the
closing commit `ad675ae4` differs from the treatment tree in documents
only. Both carry ADR 606's `--query` driver extension and the
diagnostic receipt (`TORON_REORIENTED_CAP_CHECK_RECEIPT_PATH`). No
rebuild is needed. The manifests are re-hashed on strix before each
launch.

*The eligibility gate, before the pair.* One q09 execution per arm on
strix with the diagnostic receipt, at the anchor coordinate. It
**fails closed**, and no pair runs, unless all of these hold:
- both runs pass, with digest and plan fingerprint equal to the
  anchor's q09 Toron cell;
- the route is `reoriented_inner_chain_grouped`;
- for each of partsupp, orders, and nation, a receipt names the stage
  with:
  - decision `materialized_batches`;
  - control path `bound_exchange`;
  - `equivalence_passed` true;
  - `reused` false under `Discard` and true under `Reuse`;
- part's receipt names control path `streamed_filter_exchange_attempt`
  and `reused` false on both arms, with its decision recorded.

Every receipt is recorded with its decision and path, including any
other stage's. If the receipts contradict the table above, the gate
fails: a different path or decision for any of the four, or a missing
receipt for any of them. The record then closes on the gate's findings,
with no pair, and the token is unaffected. Any revised experiment gets a
successor plan.

*The pair.* ADR 597's twelve-row paired driver on q09, run as in ADR
606:
- `--query q09`;
- `--sealed-manifest target/tpch/adr603-anchor/perf_board.jsonl.evidence.sealed`;
- ADR 592's validator on every row;
- ADR 593's readings;
- ADR 595's separated launch;
- ADR 599's gate with `--gate-settle-seconds 5`;
- one attempt.

The driver reports ADR 588's figures unchanged: the control range, the
bar `2 × range_control`, the gain per pair, the median gain, and every
pair's sign. Peak RSS and `spill_bytes` are reported for every row.

*The interpretation, predeclared.*
- **q09 effect established:** the median paired gain exceeds
  `2 × range_control` and every pair is strictly positive.
- **q09 effect not established:** any other outcome without a
  regression below. The token stays `Reuse`. This record makes no
  retain or revert decision from q09.
- **Regression; the token is revisited:** any of the following opens a
  new record on the token. The closing commit does not revert
  silently.
  - a digest mismatch on any valid row;
  - `spill_bytes` above zero, or a join-spill observation, on a
    treatment row where the paired control row has none;
  - the treatment's median slower than control's by more than
    `2 × range_control`, with every pair negative.
- Peak RSS is reported and compared, with no threshold. A q09 RSS
  increase is an observed cost beside the time result, as on q05, with
  no cause asserted from the pair alone.

*The mechanism check, after a verdict.* ADR 605's `scan-work-census` on
q09 against the treatment executable. Scans per table, predeclared:
- partsupp, orders, and nation: 1 each (2 before);
- part: 2, unchanged;
- supplier and lineitem: 1 each, unchanged;
- the progress fold reconciles with the ledger;
- the digest and fingerprint equal the anchor's;
- the route is unchanged.

The comparison is with ADR 605's q09 census, a pre-606 executable, and
the record says so.


## Acceptance

- The eligibility gate's receipts, per arm, per stage, as above. Or the
  gate's refusal, and the record ends there.
- On strix, one paired run under the declared procedure:
  - twelve rows;
  - readings;
  - separation and window verified;
  - the control range and the bar;
  - the gain per pair and its median;
  - the interpretation;
  - peak RSS and spill per row.
  Or the driver's refusal.
- After a verdict, the mechanism check as above.
- Plan commit, then one implementation commit with the results and the
  closure. No code changes are expected. If the launch scripts need a
  harness change, it is stated and reviewed before the gate.


## Guardrails

- No build. Both arms are ADR 606's executables, identified by hash.
- ADR 588's rule, the twelve-row contract, and ADR 599's gate
  procedure, verbatim. One attempt, and no relaunch under this record.
- The host snapshot scripts are generated from this record's paths.
  Every ADR-numbered path in `snapshot.sh` and `launch.sh` is replaced,
  and both are checked with a grep for stale numbers before launch,
  because ADR 606's pair recorded ADR 601's paths.
- No claim beyond q09. No 22-cell total inferred.


## Risks

- A reusable stage may take `Stream` on q09. Then the gate fails
  closed, and the finding is that stage's decision.
- The effect may sit under the bar. That outcome is "not established"
  for q09 and does not reflect on q05.
- Part's second scan is out of this record's reach. Covering it would
  need the treatment to reproduce the streamed filter-exchange path, or
  the `LIKE` filter at the scan (ADR 605's filter-placement candidate).
  That is a later record.
- q09's build and probe volumes differ from q05's, so peak RSS may
  change differently, in either direction. The pair measures it.


## Forward

- After this record: re-seal the full board on main's tree, at the
  anchor coordinate, to measure the campaign's total and ratio. Then
  choose the next owner by absolute gap. On the ADR 603 anchor, the
  largest gaps were q21 5.8 s, q09 5.5 s, q05 4.7 s, q18 4.5 s, and
  q01 3.7 s.
- Filter placement at Toron's scans (ADR 605) remains a candidate. DuckDB
  emits 3.2% of q05's lineitem and 6.5% of q09's, and Toron emits 100%.
