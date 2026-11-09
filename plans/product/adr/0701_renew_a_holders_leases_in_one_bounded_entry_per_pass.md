# ADR 0701: renew a holder's leases in one bounded entry per pass

Status: proposed, 2026-11-09 (Claude, Codex). Implementation lands with this plan. Review record: `reviews/lease-missing-startup/` (WORKING-RECORD, DELTA-1, CODEX-1).

## Why
The registered startup LeaseMissing defect candidate: an unretried first write after `ready()` stalls because its holder refuses its own child write for a missing lease.

Under parallel test load a holder's applied leases were observed to **expire while being renewed**, on main as well as before ADR 0700. Several of a holder's leases had expired together while others renewed in the same pass were valid, and a grant after the gap then closed the lineage, so an in-flight parent was Refused.

This is a concrete mechanism to fix. It does not establish that every historical failure of `racing_attempts_and_retries_follow_the_recorded_claim` had this cause.

**The mechanism** (`refresh_leases_once`):
- One pass reads the clock once. It then renews every one of the holder's primary ranges, up to `VNODES_PER_NODE` = 64 merged ranges (43 for a node of the test ring), with one serial Raft entry each.
- Every grant is stamped from that one pass-start reading: the leader's local grant and the remote grant, whose `lease_start` is the follower's `requested_hlc`. A grant issued late in a long pass is therefore already near or past its expiry.
- Renewal demand is also per range. Under the observed load, passes took 1.2 to 1.7 s, and a measured attempt at concurrent renewal (16 in flight) did not raise the cluster's grant throughput (about 30 per second under that shared load). That is not claimed as a universal ceiling.
- Per-range demand also grows with membership: there are N holders of up to 64 ranges each, and one control log.

## Decision
1. **Timestamps without stale reuse.** The time authority is unchanged: a holder's request carries the holder's clock reading, and the leader's own grants use the leader's clock. What changes is that a reading is captured immediately before the request or entry it stamps, never reused across a serial wait.
   - A batch shares one fresh reading, because its leases are constructed together without a wait.
   - Queueing and commit delay can still consume part of a grant's lifetime. That is not removed.
2. **In ParentDecisions mode, one bounded batch per holder per pass.**
   - The leader's own pass proposes one `AcquireLeases` entry.
   - A follower's pass sends one `LEASES_REQ` with all its ranges. The leader signs each lease and proposes one `AcquireLeases` entry.
   - Demand per pass is O(holders) entries when each holder fits one batch, otherwise O(chunks). Per-lease signing and validation work remains.
3. **Today keeps per-range renewal**, with only item 1's fresh stamps. Emitting the batch in Today would need a WAL compatibility change (an older Today binary would meet an unknown variant mid-replay rather than at a format gate). That is a materially wider change and is not made here.
4. The ineffective concurrency-window experiment is not kept.

## The batch
- **Bounds:**
  - `MAX_LEASES_PER_BATCH` = 64 (= `VNODES_PER_NODE`, the most merged primary ranges one holder can have, independent of membership). Each lease is 144 bytes.
  - A holder with more ranges than the cap, which the current ring cannot produce, is split into bounded chunks in range order. No range is omitted.
  - The count is checked **before allocation**, on the request and on the response alike: the RPC meta length must equal header + count × entry by checked arithmetic, with 1 ≤ count ≤ cap. The log and snapshot decode use a bounded sequence type whose deserializer refuses a length above the cap before reserving.
  - 64 is a wire and entry bound, not a membership bound. Chunking covers an unexpectedly larger range set without dropping a range.
- **The envelope** is validated whole before any element applies:
  - count in 1..=cap;
  - every lease is for one holder and one epoch;
  - no duplicate `range_start`;
  - every range is well-formed.

  An invalid envelope applies nothing (`LeaseBatchMalformed`). Empty and oversized are malformed.
- **One reducer.** The single-grant apply arm becomes `apply_lease_grant(state, lease) -> Success | LeaseDenied`, and `AcquireLease` and `AcquireLeases` both call it. The batch applies its leases **in declared order against the evolving state**, so a later Denied does not undo an earlier Success: the outcome equals the same leases as sequential `AcquireLease` entries.
  - The response is one ordered result per lease, identified by `range_start`.
  - Differences from separate entries, disclosed: every grant in a batch shares one log index and one apply, so observations keyed by entry (applied index, per-entry metrics) differ. Lease and lineage state are equal.
- **Authorization** is unchanged per lease:
  - the leader authenticates the caller as the batch's holder;
  - each range must be exactly one of the holder's primary ranges at the current epoch, as `validate_acquire_lease_target` checks today;
  - the leader must be leader, and the TTL is clamped by the same cap.

  The leader signs each lease.
- **The response** is validated whole, including the byte length before any allocation and every signature, before ANY cache write:
  - a bounded count;
  - exactly one result per requested range, in order, for the requested holder and epoch, with no duplicate or extra result;
  - every granted result a lease for that range that verifies under the issuer's key.

  A truncated or malformed response caches nothing; it is not a prefix of successes, nor evidence that its omitted leases did not apply. Within a complete, valid response, ordered per-lease denial is the allowed partial success. A lost response leaves applied grants possible, and the next pass retries through the same admission.
- **Today** applies an `AcquireLeases` entry as refused without effect, as it does a decision command. The mode is fixed at the start of a pass: ParentDecisions takes only the batch path and Today only the per-range path, with no fallback between them.
- **Compatibility: a new ParentDecisions protocol version, by the mode's existing machinery.** Supported deployment: a **fresh disposable cluster replacement**: new stores under this version. It is not a data-preserving upgrade (there is no conversion), and no rolling upgrade or mixed-version membership is supported.
  - `WendelonRequest::AcquireLeases`, the `WendelonResponse` lease-results variant and the `LEASES_REQ`/`LEASES_RESP` message types are appended, and no existing tag is renumbered. Only ParentDecisions emits them.
  - **Every ParentDecisions version gate is bumped together**, as each ADR 0699 slice that changed encodings did:
    - the store marker `parent-decisions-v5` → `v6`, with v5 retired;
    - the WAL and snapshot format 13 → 14;
    - the Raft envelope version 6 → 7.

    Today's formats, frame version and marker are unchanged.
  - **The joiner check.** A new capability bit, `ParentDecisionsBatchedLeases` (bit 3), is advertised together with `ParentDecisions` (bit 2), and a ParentDecisions cluster requires both. A joiner of an earlier ParentDecisions version (bit 2 alone) is refused before it becomes a learner, and so is a Today joiner.
  - **An earlier ParentDecisions binary (ADR 0700, format 13) against this version:**
    - **its store:** a v6 marker is refused at store admission, before the log opens and before any write;
    - **replay:** format-14 records are refused at its existing format gate, before any entry is decoded;
    - **a snapshot:** format 14 is refused at the same gate. An InstallSnapshot also travels in a version-7 frame, which it refuses first;
    - **Raft:** version-7 frames are refused before decode, in both directions;
    - **joining a version-0701 cluster:** it is refused before it becomes a learner (the bit-3 check);
    - **the residual, asymmetric case:** an earlier cluster cannot be taught the new bit, so it checks only bit 2 and its leader may admit a 0701 joiner as a learner before replication fails. Frames are refused both ways before decode. Pre-learner refusal therefore holds only for a 0701 cluster; mixed-version membership is not safe. The supported workflow never joins a new node to an old cluster.
  - **This binary against an earlier store:** a v5 store is refused at admission ("of another version"). There is no conversion.
    - **The consequence, stated:** this version runs only on fresh stores, as after every earlier 0699 slice. Existing data is not deleted: an untouched v5 store stays readable by its own binary. This ADR authorizes no reset or deletion, and tests use new isolated disposable stores. Replacing any real existing store is a separate operator decision.
    - **Rollback:** an ADR 0700 binary can reopen a retained, untouched v5 store. It cannot consume v6 state.
    - Offline readers and backup accept Today only, unchanged.
  - Today emits no batch, and its WAL, snapshot and frames are unchanged.
- **The refresh pass** stays the lease manager's single owned step, in its task: no detached fan-out, the same tick cadence, epoch retention, signature checks and incarnation and lineage gates. Parent outcomes keep their first-terminal semantics.

## Controls
- **Batch versus sequential, for WELL-FORMED envelopes**, in ParentDecisions, the only mode that emits it, at the reducer (applying the entry directly): the same leases as one `AcquireLeases` or as separate `AcquireLease` give equal leases, lineages and per-lease results. Covered cases:
  - **mixed success and denial:** a batch of canonical primary ranges cannot overlap within itself. This fixture therefore **synthesizes** an earlier live lease of another holder overlapping one of the batch's ranges, to exercise the shared reducer; it is not claimed reachable under mode admission, and the canonical-range checks of the public RPC are not weakened for it. That element is Denied and the others are granted, in both forms;
  - continuous renewal by a new issuer (same generation);
  - a gap closing the old lineage, with its parent refused through Finalize.
- **A malformed envelope applies nothing**, a separate control and not an equivalence: a duplicate `range_start`, empty, over the cap, mixed holders, mixed epochs.
- **Bounded decodes:** an oversized length is refused before allocation (the log, request and response decodes); length arithmetic is checked.
- **Responses:** association by `range_start`; a truncated, extra or duplicate result, or an unverifiable lease, caches nothing.
- **Compatibility:**
  - a v5 marker is refused, and the retired-marker controls are extended;
  - the formats are 14 and 14 and the frame version is 7, with Today's unchanged;
  - a joiner with bit 2 alone, with neither bit, or with bit 3 alone is refused before it becomes a learner.
- Today refuses `AcquireLeases` without effect. A Today pass never builds or sends a batch; it renews per range with fresh stamps, and that timestamp correction is tested on its own.
- **The RPC:**
  - a non-holder caller is refused;
  - a range that is not the holder's is refused, and nothing is signed for it;
  - the follower caches only granted, verified leases.
- **Stale construction:** a deterministic delayed-pass control, where a clock advances across the pass's waits, shows that no grant carries a reading older than its own request.
- `racing_attempts_and_retries_follow_the_recorded_claim` stays unchanged.
- **One loaded before/after observation** under the same load as the retained baseline (lm-s4, main): actual lapse events (applied leases expired at a holder's LeaseMissing), and pass latency and margin (how long before expiry each range was renewed), with passing-test output kept. A failure is examined, not rerun until green.

## Evidence
- **Focused controls (the review record):** batch versus sequential, malformed envelopes, bounded decodes, whole-response validation including status discriminants, Today and batch stamping, RPC authorization, the version and joiner gates. All pass. `racing_attempts_and_retries_follow_the_recorded_claim` passes unchanged.
- **One A/B loaded observation** (bigfoot):
  - **Setup:** the same base, script, cgroup, features and test selection; a cfg(test) diagnostic kept out of this source; one run per arm.
  - **The arms:** A = main, B = this ADR.
  - **Tests:** racing_attempts 30/30 in both arms. The background load failed only the root-size fence, which the diagnostic's own lines trip, identically in both arms.
  - **Results:**

    | | A (main) | B (this ADR) |
    |---|---|---|
    | LeaseMissing observations | 5 | **0** |
    | Pass-start remaining lifetime, p5 | 319 ms | **1197 ms** |
    | Pass-start remaining lifetime, p50 | 1285 ms | **1787 ms** |
    | Samples with a lapsed lease | 42 of 2156 | 34 of 2269 |
    | …lapsed under 2 s | 12 | 6 |
    | …lapsed over 2 s | 30 | 28 |

  - **Reading:** the long tail is unchanged and is consistent with these tests' deliberate outages and leader changes, though it is not attributed. These are approximate pass-start samples, not per-renewal margins, and neither zero observations nor a passing run proves uninterrupted lease coverage.

## Not established
- That every historical racing_attempts failure had this cause.
- That leases no longer lapse: short lapses (under 2 s) were still observed under load, as were longer ones around outages and leader changes.
- Any capacity or performance guarantee. Batching removes per-range log-entry overhead and keeps the per-range signing and validation work. Stamping just before each request or entry does not remove network or commit delay. There is no lease-TTL guarantee under a slow or unavailable quorum.
- Today still renews per range (fresh stamps only).
- General control-log throughput (not pursued).
