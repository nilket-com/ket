# ADR 0102 - Counter visibility: hide uncommitted values, witnessed bases, persisted operation origin

## Status

Accepted and implemented: a5c50bd7 (2026-10-16). The defect was proven by a deterministic test that fails on the unfixed parent 722a951b and passes on the fix.

Decision item 5 (pairing by version identity, F-CTR-6) is implemented: plan eb18e398, impl 53ec94e5 (2026-10-17). F-CTR-5a class-preserving compaction and the F-CTR-7 multi-source LSM guard are implemented: plan 162f76a7, impl beb5c7af (2026-10-17).

F-CTR-5b remains open. The durable `counter-applied.log` is length-prefixed postcard and grows by about 44 bytes per resolved add; it is append-only, has no configured size bound, and is read into memory in full during recovery. Pruning waits for an explicit retention/TTL guarantee and a crash-safe compaction design.

## Context

A counter add replicates the ABSOLUTE new value, `current = read(key) + delta`, to its replicas and records it in the leader's finalization witness.

`CounterReplicaService::read_committed` returned the latest LIVE version, and a replica tentative is live-indexed at append. So:
- **(a)** a public counter read could return an uncommitted tentative, even one later rolled back (a dirty read);
- **(b)** an uncommitted tentative could become the leader's base and be baked into a committed absolute value. Reachability was conditional.

Persisted counter versions were keyed by HLC alone (`(op_id,0,0)`), which is narrower than the operation identity (tenant × origin × op_id).

## Decision

1. **The visible and base value** is the highest-ordered eligible version: witnessed at the current owner, or marked at a replica. A raw tentative is never visible. A reader gets the witnessed view only when it is the key's current owner; otherwise it gets the marked view. The base for the next add is exactly that eligible winner, taken under the existing per-key add lock.
2. **Publication** happens synchronously after the witness write (leader) and after the marker (replica), never on tentative append. A collision check and a reservation run before the witness write. A failure after the witness poisons the key with a typed error instead of rolling the witness back.
3. **Origin is persisted.**
   - New tentative and marker record classes 3 and 4 (16 B: value or offset, origin, reserved).
   - A versioned witness file (`.pw2`) carries the origin.
   - Versions are ordered by (HLC, origin); an equal identity with different content is a typed Conflict.
   - The production service checks that HLC == (op_id, 0, 0).
   - Legacy records and `.pw` witnesses stay readable under an unknown-origin rule and are never rewritten.
4. **Maintenance refusal.** Raw migration (sender side) and compaction refuse counter records with a typed error before writing. The blob tier rejects them.
5. **Pair V2 counter frames across slab rotation.** At reopen, pair class-3 tentatives with class-4 markers by the full version identity `(tenant, key, hlc, origin)` across all local slabs, active and sealed. Slab-local offset is not a cross-slab pairing key; if a marker and tentative are in the same slab, a contradictory offset is a typed `Conflict`. Recovery is deterministic and independent of slab enumeration order. A fully paired marker contributes its identity and value to a value-bound committed map; live commits update the same map. An orphan marker fails closed for its key and never enters the map. An orphan tentative remains uncommitted and is skipped. Legacy classes 1/2 keep read-only, per-slab offset pairing because they do not persist origin. Prepare success follows durability of the exact tentative slab; a missing-pending commit succeeds only for an equal committed identity/value, returns `Conflict` for a different value, and otherwise returns generic-retryable `TentativeMissing`. Leader re-drive remains prepare then commit, and the witness remains until all accepted participants commit.

## Rationale

The witness, not the marker, is the leader's durable commitment. Using it as the base keeps accumulation exactly-once while markers are still in flight. Persisting the origin closes the identity gap at the source; pre-release format changes are permitted.

## Consequences

- **F-CTR-5b remains open.** F-CTR-5a preserves counter classes on the selected one-source maintenance paths, and the F-CTR-7 guard refuses counter-bearing multi-source LSM rewrites before output creation. Counter pruning and its retention/TTL contract remain undecided.
- Migration and tiered handling of counters are refused, not supported. Mixed-version clusters are unsupported. Residual F-CTR-1b, unknown origins on equal-value ties, applies to legacy data only.
- The eligible-version cell is memory-only, rebuilt at reopen from markers and witnesses, and pruned to the winner plus outstanding reservations.

## Amendment — F-CTR-5a class-preserving compaction (2026-10-17)

**Status:** implemented in `beb5c7af` after plan commit `162f76a7`.

### Decision

F-CTR-5a will preserve counter record classes at the three selected one-source rewrite paths: sealed-slab compaction, reclustering, and the shared retention/reclaim rewrite path. The rewrite must preserve each counter frame's class, exact payload, tenant/key/HLC/origin identity, and durability semantics. It must not reinterpret V2 pairing by slab-local offset across slabs. For a tentative and its marker that are both in the rewritten slab, the implementation must validate and apply a bounded old-to-new tentative-offset mapping to the marker; absent, conflicting, or ambiguous mappings fail closed. Legacy class-1/2 bytes remain read-only and retain their existing per-slab pairing behavior. Counter frames must prevent a replacement from being discarded merely because all ordinary rows were elided; output manifest/index accounting must represent counter-bearing replacements without fabricating ordinary rows. This work does not add counter pruning, physical erasure, migration, tiering, or mixed-version support; counter debt remains for the later 5b decision.

The class-aware source walk must be bounded and non-mutating. It must validate record header, class, length/checksum, and V2 payload before creating any replacement. It must not call `recover_slab` as an enumeration shortcut because recovery can mutate anomalous bytes. A scan/admission failure leaves source bytes and publication state unchanged.

### F-CTR-7 and multi-source LSM guard

The B1 source census identified a reachable, pre-existing LSM class-loss path. A focused reproduction on clean base `89219e7d` committed a V2 value of 42, sealed its slab, ran one L0→L1 compaction, and read the value live before and after compaction. Reopening returned typed `Unsupported`, not an incorrect value. The test-only patch and raw attempts are retained at `/home/me/work/reviews/adr0102-f-ctr-7-repro-1`.

LSM plans can touch multiple input and overlap slabs and do not enumerate unindexed commit markers. They remain outside the one-source preservation/remap scope. Before LSM output creation, any plan whose touched slabs contain counter frames must return a typed retryable fail-closed refusal, with no output publication or source/manifest mutation. The source regression test will use the same committed-counter/rotation/compaction fixture: the unfixed base accepts and rewrites the LSM job then reopens as `Unsupported`; the fixed implementation refuses before rewriting and still reopens the committed value. No blanket claim that every existing compaction path is one-source is permitted.

### Required implementation tests and gates

The F-CTR-5a source package must cover (a) ordinary-only rewrite compatibility, (b) class-preserving V2 tentative/marker rewrite, (c) valid same-slab offset remap and a disabled-remap negative, (d) cross-slab identity pairing without offset dependence, (e) same-slab offset conflict and orphan-marker fail-closed behavior with bytes unchanged, (f) legacy and malformed/undecodable frames refused without rewrite, (g) counter-only and all-ordinary-elided replacement publication/accounting, and (h) the F-CTR-7 multi-source LSM guard before any output is created. Tests must also pin duplicate marker idempotence, orphan tentative behavior, live eligible reads, and the complete recovered committed/value-bound map after reopen.

The plan commit was docs-only; its Wendelon architecture and mutation-visibility architecture tests ran before fast-forward. The separately reviewed implementation passed its focused regression suite, changed-area development validation, and reviewed ladder/external audit sequence before landing.
