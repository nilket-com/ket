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

The reproduction exposed that a normal LSM job can accept an indexed V2 tentative, omit its unindexed marker, and rewrite the tentative through ordinary `STANDARD` append. `CompactionPlan` may contain multiple input/overlap slabs, so the one-source offset-remap rule does not apply. Before output creation, inspect every touched slab for counter classes; if any are present, return a typed retryable fail-closed refusal. The refusal must leave source frames, output paths, and manifest publication unchanged. Keep this guard outside F-CTR-5a's one-source preservation scope.

The regression test will reuse the retained reproduction fixture but assert the selected fixed behavior: the base compaction call succeeds and reopen yields `Unsupported`; the fixed implementation refuses the LSM plan before rewrite and reopen still yields the committed value. The baseline reproduction itself observed no wrong value.

### Required tests and validation

Test coverage includes: ordinary-only rewrite unchanged; V2 class and payload preservation; successful bounded same-slab offset remap; remap-disabled negative; cross-slab pairing by full identity; same-slab offset conflict; orphan marker refusal with byte preservation; orphan tentative skip; duplicate-marker idempotence; legacy and malformed-frame refusal with byte preservation; counter-only/all-ordinary-elided replacement publication and accounting; live eligible view and full reopened committed/value-bound map; and F-CTR-7 LSM refusal before any output. Source validation must prove no rewrite occurs after an admission failure and no selected rewrite loses class or recovery evidence.

**Gate order:** this docs-only plan commit → Wendelon architecture and mutation-visibility architecture checks → fast-forward → focused test-first source package/review → source gates → changed-area DEV → reviewed full ladder → independent external audit → FINAL and separate joint fast-forward. Keep the F-CTR-7 multi-source guard regression in the source package; do not expand 5a into multi-source preservation.
