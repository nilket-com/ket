# ADR 0102 implementation plan: pair counter markers by version identity

**Status:** plan only; no implementation source changed in this commit.
**Base:** published `6388e2f6382fa81061a75ebe054fc4f1453576b7` (C-S3a).
**Defect evidence:** `/home/me/work/reviews/adr629-f-ctr-6-repro-1` (rotated-slab reproduction accepted with Q1 = YES).
**Contract:** `/home/me/work/.agent-bridge/wendelon-adr0102-f-ctr-6-contract-r1/CONTRACT.md` and Addenda A/B. This plan records the selected contract; source remains gated on the D1–D5 census and subsequent source review.

## Decision and identity

A V2 counter version is identified by `(tenant, key, hlc, origin)`. Recovery must collect V2 tentative and marker frames from every local slab (active and sealed) before pairing. Pairing is a deterministic global join on that identity, independent of slab enumeration order. The marker offset is ignored across slabs; when both frames are in one slab, an offset disagreement is a fail-closed `Conflict`. Legacy classes 1/2 retain read-only, per-slab offset pairing because they carry no origin. No compaction, migration, tiering, or mixed-version support is added.

An orphan marker refuses its key and is not entered into committed state. An orphan tentative is skipped. Fully paired marked versions populate a value-bound in-memory map `VersionIdentity -> value`; each live commit updates the same map. The map is rebuilt from the frame walk and is bounded by the number of paired V2 markers. For a missing pending entry, the same identity and value is an idempotent success, a different value is `Conflict`, and an absent identity returns typed `TentativeMissing`, mapped through the existing generic retryable path rather than visibility poisoning.

## Durability and retry behavior

Both replica and local tentative prepare paths must await the exact tentative slab's group-committed durability barrier after append and before indexing, pending-state insertion, or successful prepare return. A sync failure returns an error and leaves no index or pending entry; the written frame may remain as an orphan. Marker durability then completes the prepare/commit edge.

Leader recovery continues to re-drive each uncommitted participant by sending prepare and then commit. It counts a participant only after successful commit and retains the leader witness until every accepted participant is committed. A lost tentative is therefore re-prepared durably before commit is retried. Do not branch on error text, add a wire status, or route `TentativeMissing` through the visibility-poison path.

## Required census before source

The read-only D1–D5 delta must establish:

1. Every writer gives the marker and its tentative the same record-header HLC.
2. The tentative is durable before prepare acknowledgement, including rotation between tentative and marker; cite the exact flush/seal path.
3. Every reader of `recovered_counter_versions` and `recovered_counter_errors` is listed.
4. The recovery collection is O(on-disk V2 frames), with the concrete bound and orphan handling.
5. After a straddled reopen, a leader add uses the recovered local committed value as its base.

If any premise fails, stop and revise the plan before implementation. The census must also preserve the already accepted details: duplicate V2 markers are idempotent only for consistent values; same-slab offset mismatch refuses the key; cross-slab pairing uses identity; prepare sync failure creates no pending/index entry; and re-drive repairs a lost tentative before committing.

## Implementation and validation scope

After census acceptance and a separate source selection, split recovery into per-slab collection and global identity pairing. Preserve bytes and behavior for legacy records and `.pw` witnesses; do not rewrite existing records. Add or adapt focused tests for the four reproducer cases (replica value 41, leader-local value 42, same-offset decoy 99 excluded, and same-slab control), orphan marker refusal with unchanged bytes, duplicate identity/value handling, order-independent recovery, legacy 1/2 compatibility, durable-prepare success and sync failure, already-committed retry after supersession, missing tentative refusal, the straddled-reopen leader add (42 → 43), and prepare-then-commit re-drive. Keep existing compaction, migration, and tier-refusal tests unchanged.

**Gate order:** plan commit → D1–D5 census delta → unsigned source and pre-seal/guard checks → source review → sign and SOURCE clearance → focused probe → changed-area DEV → reviewed full-ladder plan/run → external audit → FINAL → joint fast-forward. C-S3a's Bigfoot gates retain priority; F-CTR-6 implementation/build/test work stays off Bigfoot and must not start until its own source clearance.

## F-CTR-5a plan: class-preserving counter compaction

**Status:** plan only. This section amends the earlier F-CTR-6 pairing plan; production behavior still follows the current typed maintenance refusal until a separately reviewed source change lands.
**Plan base:** published status-closure commit `89219e7df69b7686e14155747b285912488dfa57`.
**Finding:** F-CTR-7 is a pre-existing LSM class-loss path. The baseline reproducer and its attempt records are retained at `/home/me/work/reviews/adr0102-f-ctr-7-repro-1`; the accepted reproduction returned live value `42` before and after one L0→L1 rewrite and `Err(Unsupported)` after reopen, with no wrong value observed.

### Selected preservation scope

Preserve physical counter classes and exact payload bytes at the three selected one-source rewrite paths: sealed-slab compaction, reclustering, and the shared retention/reclaim rewrite path. Keep ordinary-row policy behavior unchanged. A bounded non-mutating physical scan must inspect all relevant source frames, validate header/class/length/checksum and V2 payload, and finish admission before any output file is created. Do not use the ordinary `key_index` alone as the frame inventory, and do not call `recover_slab` to enumerate because that recovery path can mutate anomalous bytes.

For a rewritten slab, carry V2 tentative and marker frames as class-aware records. If both members of a V2 pair are in that slab, validate the original same-slab offset relation and remap the marker to the rewritten tentative's offset through a bounded mapping. Cross-slab pairing remains keyed by `(tenant, key, hlc, origin)` and ignores offsets. Conflicts, malformed frames, missing or ambiguous same-slab remaps, and unsupported legacy rewrites fail closed with source bytes and publication state unchanged. Legacy classes 1/2 keep their existing read-only per-slab offset semantics.

Counter-bearing input prevents the no-replacement deletion path even when ordinary retention/tombstone policy elides every ordinary row. Replacement index and manifest accounting must describe the preserved counter-bearing data without inventing ordinary rows. The implementation must keep live key locations and eligibility coherent across replacement and prove the complete value-bound committed map after reopen. This plan does not authorize counter pruning, physical erasure, migration, tiering, mixed-version operation, or 5b growth/pruning.

### F-CTR-7 multi-source LSM refusal

The reproduction exposed that a normal LSM job can accept an indexed V2 tentative, omit its unindexed marker, and rewrite the tentative through ordinary `STANDARD` append. `CompactionPlan` may contain multiple input/overlap slabs, so the one-source offset-remap rule does not apply. Before output creation, inspect every touched local slab for counter classes; if any are present, return a typed retryable fail-closed refusal. Accepted L2 blobs are counter-free in this release: `recover_slab_entries_from_blob` rejects record classes 1–4 before tier publication or L2 recovery. Ordinary L2 inputs therefore pass this guard and remain eligible for normal LSM compaction.

A counter deferral records the pending-frame-set size in the debug event and keeps the existing retryable cooldown. For L0, the picker excludes only the deferred counter-bearing source from its next candidate, provided the remaining ordinary run count still meets the configured L0 threshold; this lets unrelated ordinary L0 runs proceed rather than repeatedly selecting the same blocked set. The refusal leaves source frames, output paths, and manifest publication unchanged. This guard remains outside F-CTR-5a's one-source preservation scope.

The pending-frame registry is volatile. If a leader abandons a tentative without commit or rollback, its entry stays live and can defer same-slab work until restart or a terminal transition. Restart clears the registry; the on-disk orphan is then eligible for the class-preserving single-slab rewrite. The debug count on deferral makes a stuck live entry visible.

The regression tests will reuse the retained reproduction fixture but assert the selected fixed behavior: the base compaction call succeeds and reopen yields `Unsupported`; the fixed implementation refuses a counter-bearing L0 plan before rewrite, counts the deferral, then compacts coexisting ordinary L0 runs on the next picker tick. A separate test proves an ordinary tiered L2 run compacts with local L0 runs. Restart coverage proves a live pending entry is cleared and its orphan slab is compactable after reopen. The baseline reproduction itself observed no wrong value.

### Required tests and validation

Test coverage includes: ordinary-only rewrite unchanged; V2 class and payload preservation; successful bounded same-slab offset remap; remap-disabled negative; cross-slab pairing by full identity; same-slab offset conflict; orphan marker refusal with byte preservation; orphan tentative skip; duplicate-marker idempotence; legacy and malformed-frame refusal with byte preservation; counter-only/all-ordinary-elided replacement publication and accounting; live eligible view and full reopened committed/value-bound map; and F-CTR-7 LSM refusal before any output, with bounded ordinary-L0 progress, tiered ordinary compaction, and restart clearing of abandoned volatile pending state. Source validation must prove no rewrite occurs after an admission failure and no selected rewrite loses class or recovery evidence.

**Gate order:** this docs-only plan commit → Wendelon architecture and mutation-visibility architecture checks → fast-forward → focused test-first source package/review → source gates → changed-area DEV → reviewed full ladder → independent external audit → FINAL and separate joint fast-forward. Keep the F-CTR-7 multi-source guard regression in the source package; do not expand 5a into multi-source preservation.

## F-CTR-8 plan — crash-safe counter dedup finalization

**Status:** plan only; this amendment authorizes no implementation source or test changes.
**Plan base:** current published ADR 0102 status after F-CTR-5a/F-CTR-7 (`b7280e6a`).
**Accepted contract:** `/home/me/work/.agent-bridge/wendelon-adr0102-f-ctr-8-contract-r1/CONTRACT.md` (r1 with Algebra/ownership and participant-durability precondition).
**Read-only census:** `/home/me/work/reviews/adr0102-f-ctr-8-census-r1/CENSUS.md`.

### Finding and identity scope

The current counter witness and applied-log paths use ordinary writes/rename/append and unlink without the file and directory durability barriers needed for power-loss recovery. A crash can lose an applied append while preserving witness removal, leaving a retry without the original outcome. The census records the exact source paths and confirms the applied log is append-only with no TTL/pruning policy; F-CTR-5b remains separate.

`CounterWriteKey` is the canonical 24-byte `ClientWriteId` identity `(client_uuid, sequence_no)`; it excludes TTL and padding. The effective durable identity is `(node-owned leader store, tenant_id, CounterWriteKey)`: production creates a leader per tenant and stores both witnesses and `counter-applied.log` in that tenant's directory under the local slab data directory. The applied frame currently contains only `op_id`, write key, and outcome, so the store directory must remain private to one local origin and must not be shared or reassigned across origins. A future shared-store or cross-origin migration must add origin to the durable namespace before proceeding. B1d's replica identity `(tenant, origin_node, op_id)` remains a separate scope. The per-tenant witness directory and applied log are the sole dedup authority; in-memory maps are recovered views.

The local and remote participant paths currently await a durable commit-marker barrier before acknowledging commit, and the leader appends its applied record only after participant commits return. This is a contract precondition, not a protocol change. If evidence shows an acknowledgement can precede durability, stop and report the gap separately as a new finding (next free F-CTR id); do not expand F-CTR-8 silently.

### Selected durability behavior

For every add that may commit a participant, maintain at least one valid durable representation of its exact `{op_id, write_key, previous, current}` outcome: a pending witness or an applied record.

1. **Durable prepare before commit fan-out.** Write a complete witness to a temporary file; sync the file; atomically rename it to the final path; sync the containing directory. If any prepare barrier fails, send no participant commit. Newly created witness-directory names must be durable before use. Use the same replace protocol for witness progress updates.
2. **Durable applied record before witness removal.** After all accepted participants have durable markers, serialize append operations so length-prefixed frames cannot interleave. Append one complete applied outcome, sync log data, and sync the containing directory if the log was newly created. Do not remove the witness unless every required barrier succeeds. A failed append/sync keeps the witness available for retry/recovery.
3. **Durable witness removal.** Unlink resolved witness paths only after the applied record is durable, then sync the containing directory. Do not report durable resolution until the directory sync succeeds. If cleanup fails, retain/reconstruct unresolved in-memory state; the applied record remains the dedup authority. Recovery from both records is idempotent, while conflicting identities or outcomes fail closed.

The counter-specific injectable file-ops seam covers temporary and append writes, file syncs, rename, unlink, directory sync, reads, and enumeration. Tests use a deterministic persistence model with volatile bytes/names separated from durable bytes/directory entries. Its crash operation discards unsynced bytes and restores names from the last directory sync. It must reproduce the old lost-applied/persisted-unlink image as a negative control and show that the selected barriers make this image unreachable after a participant commit. This is a logical crash model, not a physical power-cut claim.

### Required tests and performance evidence

Tests must use the real `CounterLeader` path, real recovery, and a retry with the same write key. Cover: prepare failure/cuts after temp write, file sync, rename, and directory sync; commit-to-applied cuts after every participant marker, append bytes, `sync_data`, and newly-created-log directory sync; applied-to-unlink cuts before unlink, after unlink/before directory sync, and after directory sync; idempotent re-resolution and op-id high-water across restarts; file/directory sync failures; conflicting duplicate refusal; concurrent non-interleaved appends for distinct keys; and existing duplicate replay, partial-commit recovery, and fresh-op HLC ordering. A torn final frame may be tolerated only while a durable witness remains. Do not use process-crash-only testing as a substitute for the persistence model.

Measure end-to-end resolved adds/second and per-add latency before and after using the same release build, machine/filesystem, participant/quorum shape, unique-key workload, operation count, and warm/cold conditions. Repeat runs and report medians and sync counts. The measurement must exercise the real file barriers and cannot replace correctness tests. No threshold has been chosen; review the measured impact before accepting performance. Group commit is not required for r1 correctness and is excluded from the initial implementation. Propose it separately if measurements warrant it, preserving all represented witnesses until a batch sync succeeds.

### Scope and gates

F-CTR-8 does not authorize applied-log pruning/TTL, compaction, wire-format changes, runtime enablement, or group commit. The current indefinite-retention policy remains until F-CTR-5b selects a retention guarantee and crash-safe pruning design.

**Gate order:** F-CTR-8 plan commit → read-only census delta → unsigned source and pre-seal/guard checks → source review → sign and SOURCE clearance → focused crash-cut tests → changed-area DEV → reviewed full ladder → independent external audit → FINAL → separate joint fast-forward. This plan commit precedes the C-S6 compromise-audit/runbooks plan commit.

## F-CTR-9 plan: a counter write key names exactly one add

**Status:** plan only. This amendment authorizes no source or test changes.
**Plan base:** F-CTR-8 implemented (`6cdea9f1`).
**Accepted contract:** `/home/me/work/.agent-bridge/wendelon-adr0102-f-ctr-9-contract-r2/CONTRACT.md`, r2 with Addendum A (K5) and Addendum B (per-operation table identity).
**Defect evidence:** `/home/me/work/reviews/adr0102-f-ctr-9-repro-r1/REPORT.md`. Deterministic leader and client tests fail at `e89fcfa3`.

### Finding and identity scope

The leader treats any known `CounterWriteKey` as a retry and returns the recorded outcome, without comparing the counter key or the delta. The recorded outcome carries neither.
- A caller that reissues `(client_uuid, sequence_no)` loses its delta silently and can receive another counter's value. Any `with_client_uuid` caller reissues it across instances, because the SDK sequence restarts at 1.
- Two concurrent adds sharing one write key on different counter keys both pass the dedup check, because the counter-key lock does not cover them. Both apply. On the F-CTR-8 base the second resolution is refused, its witness stays pending, and the next recovery fails closed for the tenant.
- An add whose durable witness is written but whose in-memory record is not yet installed can be applied twice.
- Shipped Toron is not exposed: its TLS client mints a fresh identity per process. The leader defect is independent of Toron.

Within one `(node data directory, tenant)` leader store, a write key W names exactly one logical add `(counter_key, delta)`. The recorded add holds the full counter key bytes (absent for legacy unkeyed frames) and the exact delta `current − previous`.

### Selected behavior

1. **K1, retry.** A known W with the same key and delta returns the original outcome.
2. **K2, reuse.** A known W with a different add is refused with the new `WRITE_KEY_CONFLICT` status (24). Nothing is applied and no recorded outcome is returned, live, after restart and in reconcile. A legacy unkeyed record cannot prove which add it records, so its W is always refused.
   - Check order: primary → key eligibility → per-write-key admission → record lookup → `delta == 0` → overflow.
3. **K3, one add per write key.** A write-key gate is taken before the counter-key lock (lock order: write-key gate → counter-key lock → participant gates → store mutexes).
   - The record is installed immediately after the F-CTR-8 durable prepare and before publish. Every rollback returns before the install.
   - The gate is reclaimed once it is no longer shared.
4. **K4, client identity (liveness).** The SDK mints a UUIDv7 per `Client`. Toron no longer uses `worker_id` as a write identity. Violating K4 causes refusals, never silent loss.
   - **Addition beyond contract r2 (Addendum B).** With the v7 default, a default table write or delete would carry the Client's v7 uuid, and ADR 542 would expire it once the Client is older than its TTL (`network/mutation_outcomes.rs:314-323`, `network/data_server/mutation_protocol.rs:242-268`). The SDK therefore mints a fresh UUIDv7 `ClientWriteId` for each table write and delete when no caller id is supplied. Counter adds keep the per-Client uuid (K2/K4). Test: a Client created more than one TTL ago still writes and deletes rows; with a v7 per-Client uuid and no per-operation identity, this test is red.
5. **K5, install atomicity under cancellation.** After the primary and eligibility checks, one spawned task owns every guard and step. A dropped caller cancels nothing.
6. **Wire and clients.**
   - Status 24 travels with zero values. The SDK surfaces it as `WriteFailed`, with no new client error variant, and checks that every OK or DUPLICATE receipt carries its own delta.
   - Toron's four flush arms treat a conflict as a definite identity fault: they drop the delta, never merge, and latch Degraded.

### Required tests

Every law test is red at 6cdea9f1 (the plan base), or with its named hook disabled, and the K1 retry controls stay green before and after. The tests cover:
- conflict on another key, on another delta and on zero delta;
- conflict against a pending witness, live and after restart;
- conflict against a keyed applied record and against legacy unkeyed records;
- the window after the durable prepare;
- deterministic concurrent same-write-key admission: on another counter key (blocked, then conflict), on the same key with the same delta (replay), and on the same key with another delta (conflict), followed by reconcile with no pending witness; plus a gate-bypass control;
- gate reclamation, with a keep-entries control;
- prepare-failure rollback;
- two operations under one write key fail recovery closed, with a typed error;
- caller cancellation at the witness write, at every store step and at a participant commit, with a marker-eligible participant. An inline-body control reproduces the lost delta.
- a structural architecture guard on the install order (an addition beyond the contract, from source review);
- wire status, SDK mapping and receipt checks; v7 client identities; per-operation table identities for a long-lived client; and the Toron flush arms.

### Scope and gates

F-CTR-9 does not authorize pruning, retention, compaction or a participant protocol change. Dedup across a ring-owner change is F-CTR-10, a known gap carried from F-CTR-8. F-CTR-5b's time-bounded retention depends on K4 and lands after F-CTR-9. Its expiry rule uses issue time plus L, but the counter uuid is minted once per Client, so F-CTR-5b must rotate the uuid before L (its accepted contract r3, law E6). Otherwise long-lived clients' counter adds would expire, which is the same hazard as Addendum B.

**Gate order:** this plan commit → Wendelon architecture and mutation-visibility architecture checks → fast-forward → signed source → independent source review → focused and changed-area tests → reviewed full ladder → independent log audit → fast-forward. The Codex-era SOURCE clearance, DEV and FINAL steps are folded into the independent source review, the changed-area tests and the log audit under the current working model (Claude implements; an independent reviewer that did not write the code clears source and evidence).

## F-CTR-5b plan: bounded counter dedup with a time-bounded identity proof

**Status:** plan only. This amendment authorizes no source or test changes.
**Plan base:** F-CTR-9 implemented (`63cdca83`).
**Accepted contract:** `/home/me/work/.agent-bridge/wendelon-adr0102-f-ctr-5b-retention-contract-r3/CONTRACT.md` (r3, after reviews of r1 and r2).
**Surveys:** `/home/me/work/reviews/adr0102-f-ctr-5b-survey-r1/SURVEY.md` (with its correction) and `/home/me/work/reviews/adr0102-f-ctr-5b-survey-r2/SURVEY.md` (pre-source, at `63cdca83`).

### Finding

The applied log is append-only and recovery reads it whole. Pruning a record is safe only if every later presentation of its write key W is refused. r0's per-client sequence watermark was unsound:
- sequences restart per client instance;
- no client uuid is ever retired;
- the watermark is node-local.

### Selected proof

The proof uses the UUIDv7 issuance time embedded in W, plus a durable per-store issuance cutoff `Q`.

- **E0, identity form.** A counter add whose uuid is not v7, or whose issuance time is more than S in the future, is refused with the new `WRITE_IDENTITY_INVALID` (25).
- **Check order:** primary → eligibility → E0 → write-key gate → lookup → E1 → K1/K2 → `delta == 0` → overflow. This supersedes the F-CTR-9 order above.
- **E1, no reapply.** Evaluated after the write-key gate and after the record lookup. An identity issued at or before `max(Q_mem, now − E)` gets `WRITE_OPERATION_EXPIRED` (22) with commit knowledge INDETERMINATE, whether or not a record exists.
- **E3, prune rule.** Compaction prunes only records with `issued(W) ≤ Q_new`, or non-v7 records, and never one whose witness file still exists. Q never decreases, and it is published to memory before any eviction.
- **Safety theorem.** Safety does not depend on L, H, S, clock skew, a backwards wall clock, or a later change to E. Those affect liveness only.
- **Defaults.** H = 300 s, S = 60 s, `L_sdk` = 3600 s, E ≥ L_sdk + H + S. Compaction uses `now_c = min(now, max_issued_accepted + E)`, so a forward clock jump cannot over-advance Q. `L_sdk` is a single constant in `wendelon::schema`, re-exported by the SDK. The server refuses out-of-range values at startup.
- **E6, SDK incarnation.** The SDK rotates its counter incarnation (a fresh v7 uuid, sequence restarting at 1) before `L_sdk`, atomically with allocation. It never sends an attempt older than H; a suppressed attempt returns `MutationStopped{KNOWN_NOT_COMMITTED}` if none was sent, `INDETERMINATE` otherwise.
- **Clients.** The SDK maps 22 alone to `MutationStopped{INDETERMINATE}`, never retried, and 25 to `WriteFailed{25}`. Toron treats 22 as an ambiguous drop and latches Degraded. Status 25 maps to a terminal error.
- **Scope.** Dedup across a ring-owner change inside the horizon remains F-CTR-10.

### Compaction

- Generations `counter-applied.<g>` carry a header: generation, Q, and the op-id high-water mark.
- Compaction is two-phase. The snapshot and the tail copy happen under the applied-log mutex; the generation write and sync happen outside it.
- `CURRENT = {g, base_len, blake3(base)}` is published by temp file, sync, rename, then directory sync. Only after the `CURRENT` directory fsync has succeeded is generation 0 replaced with a poison frame that old parsers reject.
- Recovery: if `CURRENT` is present it is the sole authority; delete numbered generations other than it and any `.current-tmp`, and ignore `counter-applied.log`. If `CURRENT` is absent, a valid (non-poison) generation 0 is authoritative and every numbered generation is deleted. If `CURRENT` is absent, recovery fails closed when generation 0 is poisoned, or when it is absent and a numbered generation exists. If all three are absent, the store is empty. Recovery seeds `Q_mem` from the header (0 for generation 0) and `op_seq` from max(header high-water, frames, witnesses) + 1 before serving any add.
- Pending witnesses are never pruned or evicted.
- A failure at or after the `CURRENT` rename latches Unsettled and trips the distrust fence.

### Implementation notes from the pre-source survey

- **Test identities.** No production path sends non-v7 counter adds. About 135 test call sites use helpers such as `wk(n)` that build non-v7 keys. These become deterministic per-process v7 keys, with the index in the sequence slot.
- **Clock and config.** The counter leader reads no clock today. It takes the data server's existing `PhysicalClock`; test constructors default to `SystemClock` and the default retention config, unchanged. Config is a validated `CounterRetentionConfig` on `NodeBootstrapConfig`, wired through `network/data_server/counter.rs`, because the data-server root is at its cap.
- **Shared v7 decoder.** It lives in `schema/core.rs`; the copies in mutation outcomes, CTAS and the SDK delegate to it.
- **SDK clock seam.** The SDK needs a new clock seam supplying a monotonic instant and unix milliseconds for E6 tests.
- **Architecture guards.** The existing guards on `recover_applied`, the single install point and the `fetch_add` region stay as strict. A new guard ensures compaction never inserts idempotency records and evicts only after the `CURRENT` publish and the `Q_mem` store. The `admitted_add` ordered-marker guard is extended with the E1 check between `recorded_answer(...)?` and `NonZeroU64::new(delta)`, and E0 is pinned before `admit_write_key`. Negative control: moving E1 before the lookup fails the guard.
- **Crash model.** Extend the persistence model's name filters and fault set (today exact names only, `crash_cut_tests.rs:117-125`) to cover generation files, `CURRENT` and `.current-tmp`: torn append, `sync_data`/`sync_all` failure, rename, and directory-sync failure for each. Each new fault must make its T-E5 or T-PUB-FAIL cut red when its barrier is removed.

### Required tests

Every safety-bearing test has a negative control. The contract table lists them all; in summary:
- E0;
- E1, including boundaries;
- the race between lookup and compaction;
- the prune rule, including unkeyed v7 records;
- op-id high-water across compaction;
- a configuration change and a backwards clock;
- every crash cut of compaction, including the first compaction, and nested cuts in recovery;
- appending after compaction;
- publication failure;
- stale-file cleanup;
- a missing `CURRENT`;
- the downgrade poison;
- a foreign write (Diverged);
- a pending witness older than E;
- bounded state and recovery read;
- SDK rotation;
- wire mapping;
- configuration validation;
- the F-CTR-9 retry controls.

Measure add latency with compaction running against without, using the F-CTR-8 harness. The ladder gains counter stress steps, because both the F-CTR-8 and F-CTR-9 audits noted that the counter concurrency tests ran only once, on one test thread. The steps are `storage::counter` at `--test-threads=8` in each of the 10 lane-B iterations, plus a nextest `--stress-count 20` pass over `storage::counter` and the data-server `counter_write_key` tests, covering T-RACE, T-K3 and T-K5. Any failure is a defect to reproduce, never a flake to retry. The steps are added to the contract's tests and replace none of them. They are delivered as a separately reviewed driver revision (gate tooling), not in this plan.

### Scope and gates

No participant protocol change. No group commit. No change to the F-CTR-8 progress-rewrite policy, which is the operator's decision.

**Gate order:** this plan commit → Wendelon architecture and mutation-visibility architecture checks → fast-forward → signed source → independent source review → focused and changed-area tests → reviewed full ladder → independent log audit → fast-forward.
