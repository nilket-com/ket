# ADR 0102 - Counter visibility: hide uncommitted values, witnessed bases, persisted operation origin

## Status

Accepted and implemented: a5c50bd7 (2026-10-16). The defect was proven by a deterministic test that fails on the unfixed parent 722a951b and passes on the fix.

Decision item 5 (pairing by version identity, F-CTR-6) is implemented: plan eb18e398, impl 53ec94e5 (2026-10-17). Class-preserving compaction (F-CTR-5) remains open.

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

- **F-CTR-5 is open.** Compaction previously rewrote counter records as plain records; now it refuses them. Class-preserving compaction is later work.
- Migration and tiered handling of counters are refused, not supported. Mixed-version clusters are unsupported. Residual F-CTR-1b, unknown origins on equal-value ties, applies to legacy data only.
- The eligible-version cell is memory-only, rebuilt at reopen from markers and witnesses, and pruned to the winner plus outstanding reservations.
