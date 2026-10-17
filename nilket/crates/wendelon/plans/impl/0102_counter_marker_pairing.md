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
