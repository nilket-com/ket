# Wendelon ADR 076: Replicated Counter Fetch-Add

**Status:** Promoted (cross-ring council, 3-of-3 on the spine; one consumer-side divergence resolved by
grounding — see Council History). Migration: `plans/impl/076_replicated_counter_fetch_add.md`.
**Seed:** the cut-8b ledger entry of ADR 075's impl plan, promoted to a full council cycle exactly as that
plan requested (*"this wants a careful design pass (and ideally a steer) before building; bulldozing the
witness rework risks a durability bug"*). The operator's steer set the urgency: **rf = 3 is table stakes**,
and until this cut, Toron ADR 207's durable optimizer evidence is latched dark on every standard cluster.
**Name:** completes the 075 pair — `idempotent_counter_fetch_add` (rf=1, exactly-once under retry) →
`replicated_counter_fetch_add` (rf>1, durable-quorum under replication): the two contracts the primitive
needed, named in sequence.
**Category:** Wendelon storage-ring substrate ADR — data-ring write primitive completion. No new wire
operations, no Control-Ring RPC, **no Toron production change**. ADR 208 (shape-scoped advisory) was
deliberately sequenced ahead of this cut as the guardrail before this amplifier.
**Placement:** Wendelon sequence (relocated at promotion from the ADR 209 cross-ring drafting thread, the
071/075 precedent; Toron 209 stays reserved for outer-join completion).

## Summary

ADR 075 shipped idempotent atomic fetch-add for `rf = 1` and honestly fail-closed `rf > 1`
(`COUNTER_REPLICATION_UNSUPPORTED` before mutation), because cut-4's single-node leader would otherwise make
an add durable only on the primary — silently violating 075-D3's durable-quorum. ADR 076 finishes the D3
contract: counter adds under replication fan out to a **ring-derived, per-key participant set**, reach
**majority durable quorum**, persist a **peer-identity finalization witness** (the load-bearing rework:
positional indices cannot be replayed safely across ring changes), complete commit markers across the
**full accepted set**, and the fail-closed guard is retired from the healthy path. The transport for all of
this — counter replicate/rollback frames, the network bridge — landed dark in 075 behind
`#[allow(dead_code)] // constructed in cut-8b`; this cut constructs it.

## Grounding (verified against the code)

- The reject to retire: `handle_counter_add` fail-closes `replication_factor() > 1` before mutation
  (data_server.rs:7708-7740), its comment naming this cut. Toron's 207 consumer latches that status
  (`runtime_evidence.rs:224`) — replicated clusters keep optimizer evidence local-pending.
- Landed and waiting: `CounterReplicaNetworkTransport` (data_server.rs:288); `replicate_counter_to_remote`
  :3862, `rollback_counter_to_remote` :3882, `counter_round_trip` :3903 (all dead-code pending
  construction); the remote-participant trait already ships frames to `peer: NodeId` (counter.rs:875-891) —
  the network layer is identity-addressed end-to-end.
- The op-id discipline is landed for this cut: tentative/commit frames carry the leader-assigned `op_id`;
  `AppliedCounterAdd.op_id` re-seeds the allocator in `recover` so re-minted `commit_hlc_for(op_id)` cannot
  break stale-reconcile ordering (counter.rs:1317-1345, a 075 code-review fix made *for* cut-8b).
- **The three positional assumptions this cut retires:**
  1. `CounterFinalizationWitness { accepted: Vec<usize>, committed: HashSet<usize> }`
     (counter.rs:1150-1152) — indices into a fixed participant vector; a recovered witness re-drives
     commit markers by index, so a ring-epoch change mid-reconcile can target the wrong peer.
  2. `CounterLeader { participants: Vec<P>, quorum }` fixed at construction (:1276-1315);
     `counter_leader_for` builds `vec![Local]`, quorum 1, per tenant (data_server.rs:7580-7627).
  3. `CounterLeader::read` resolves local storage as `self.participants[0]` (:1551-1556) — a hidden
     local-is-index-0 convention.
- The witness is **durable postcard** (`counter-witness-*.pw`, atomic temp+rename, :1191-1198): the
  index→identity change is a durable-format change, and `#[serde(default)]` does not stabilize postcard
  encodings — evolution must be explicit.
- Discovery exists: `replicas_for_locality(locality) -> Vec<NodeId>` (ring.rs:148/:355, trait at
  data_server.rs:786); `owner_of_identity` (:199) expresses the same first-replica primary rule that this
  cut must enforce inside the counter handler.
- Ordinary data-ring writes use the majority floor `quorum = (rf / 2) + 1` (data_server.rs:10246) —
  counters inherit the cluster's existing durability rule rather than inventing a counter-only one.
- Two-node epoch helpers exist (`advance_to_two_node_epoch` in tests/client_data_api.rs and
  tests/data_ring_wire.rs), but the headline replication fixture must exercise a real remote
  data service/transport path (simulated transport or an equivalent multi-service loopback), not only a
  single-node server with a two-node ring snapshot.

## Decisions (promotion-locked)

**D1 — Same public surface.** Reuse 075's `COUNTER_ADD_REQ/RESP`, `COUNTER_READ_REQ/RESP`,
`COUNTER_REPLICATE_REQ/RESP`, `COUNTER_ROLLBACK_REQ/RESP`. No new opcodes, no registry, no blob-write
variant, no client-facade signature change, no Toron-facing API change.

**D2 — Peer-identity witness, version-tagged.** `CounterFinalizationWitness` records participant
**identities** — `accepted: Vec<NodeId>`, `committed: HashSet<NodeId>` — inside an explicitly
version-tagged durable envelope. The local arm is recorded by this node's `NodeId`, never index 0. This is
the load-bearing correctness boundary: a recovered witness stays meaningful after the ring's replica order
changes. Postcard/serde defaults are never load-bearing for format evolution again.

**D3 — Fresh routing and recorded-peer reconciliation are distinct routes; the local arm is explicit.**
The leader stops holding a fixed `Vec<P>`. Two resolution routes that cannot be cross-called: *fresh add* →
participants derived per add from the current ring for the key's locality (explicit local arm + remote
participants for the other replica identities); *witness re-drive* → resolves **exactly the recorded
`NodeId`s**, using the current transport only to reach those identities — never mapping a recorded position
through the current ring. `read` uses the explicit local arm (positional assumption #3 retired).

**D4 — Fresh adds are ring-derived and primary-routed.** `handle_counter_add` derives the counter identity,
resolves the key's current replica set, and treats the first replica as the primary owner (the same
discipline `owner_of_identity` expresses over the full ring). A non-primary returns the existing
`NOT_LEADER` status with that owner as the hint. This is new handler-side enforcement for counters — the
current 075 handler relies on client routing and does not yet perform the owner gate itself. Effective rf is
the length of the key's current replica list (`rf.min(data_count)` discipline preserved).

**D5 — Quorum is the majority floor; completion is the full accepted set.** Quorum =
`(effective_rf / 2) + 1`, matching ordinary data-ring writes. **Accepted set ≠ quorum set**: once quorum is
reached and the witness is durable, rollback is no longer legal and the commit fan-out (plus reconcile)
must finish markers for *every* recorded accepted peer, including acceptors beyond the quorum count.

**D6 — v0 witness migration: total, versioned, hostile to impossible shapes.** Unversioned (v0) witnesses
can exist only from the rf=1 implementation, where the only valid shape is `accepted == [0]` with
`committed` empty or `{0}`. On load: that shape migrates to the local node's identity. **Any v0 witness
containing a non-zero participant index fails closed as invalid/corrupt** — refused with the file named,
never guessed into a peer identity.

**D7 — Post-quorum recorded-peer unavailability stays pending.** After quorum + witness persistence the add
is accepted; an unreachable recorded peer's commit marker is a pending completion — patient bounded-backoff
retry across reconcile passes, witness visible, never expired, never rolled back. Permanent peer loss /
counter re-replication is a named deferral, not silent expiration.

**D8 — The rf>1 fail-closed branch is retired from the healthy path.** Healthy replicated clusters never
see `COUNTER_REPLICATION_UNSUPPORTED` from `COUNTER_ADD_REQ`. The status constant remains for wire
decoding compatibility; the degraded-node fail-closed branch above it is untouched. Real failures use the
existing closed statuses (`NOT_LEADER`, `INTERNAL_ERROR`) without mutation. The deletion lands only
alongside the passing rf>1 behavioral net (Integration Fixtures below).

**D9 — Zero Toron production change; no auto-unlatch.** Toron 207 already writes/reads these keys; this cut
makes the writes accepted under replication. Coordinators that latched `ReplicationUnsupported` before the
upgrade recover by **process restart** (the 207 latch is process-sticky by design — its R1 review fix made
acks unable to clear it); a rolling restart after the storage upgrade re-lights the lane. Auto-unlatch
machinery is explicitly not part of this cut.

## Integration Fixtures (required; two-node harness)

- **Headline — `replicated_counter_fetch_add_concurrent_adds_sum_exactly`:** rf=2 over two live data
  services, N concurrent `counter_fetch_add(key, 1)` with unique write_ids → `counter_read` = N; survives a
  leader restart mid-stream; a duplicate write_id replays identical `previous`/`current` and adds once.
  **Fails on `main`** (the reject fires). The fixture must observe the remote counter replicate path, not
  only a local participant under a synthetic two-node ring.
- **`reconcile_after_ring_epoch_shift_targets_recorded_peers`** — the index-vs-identity discriminator:
  pending witness persisted, ring epoch shifted so positional indices re-map, recover + reconcile → commit
  markers land on the **recorded** peers. Fails under positional re-drive.
- **`fresh_add_uses_new_ring_while_redrive_uses_recorded`** — the D3 boundary in one fixture: post-shift,
  a fresh add resolves the *new* replica set while the recovered witness re-drives the *old* recorded
  peers.
- **`failed_quorum_rolls_back_all_accepted_rf2`** — 075's conditional no-poison fixture, now
  unconditional: every accepted tentative rolls back; the next add's `previous_value` is unpoisoned.
- **`commit_fanout_completes_beyond_quorum_rf3`** — D5's completion ≠ quorum: with three acceptors and
  quorum two, the slow third still receives its marker; the witness resolves only at full completion.
- **`no_primary_only_success_under_replication`** — no path returns OK for rf>1 with only the local arm
  accepted (the exact failure 075's fail-closed guard existed to prevent, now pinned positively).
- **`v0_local_witness_migrates_v0_remote_index_fails_closed`** — D6's totality, both halves.

## Architecture Guards (promotion-locked)

- `adr076_witness_records_peer_identities_not_indices`
- `adr076_witness_format_is_versioned`
- `adr076_fresh_and_recorded_resolution_are_distinct_routes`
- `adr076_counter_leader_has_explicit_local_arm`
- `adr076_fresh_counter_participants_are_ring_derived`
- `adr076_counter_quorum_uses_majority_floor`
- `adr076_commit_completion_covers_full_accepted_set`
- `adr076_pending_witness_never_expires_on_peer_loss`
- `adr076_replication_unsupported_not_on_healthy_path`
- `adr076_no_new_wire_operations`
- `adr076_no_toron_production_change`

**Reconciled prior guard (named retarget, discipline #4):** `adr075_counter_add_fails_closed_under_
replication` — its purpose (no primary-only durability under rf>1) is preserved, its mechanism inverts:
it now asserts rf>1 adds build replicated participant sets and the unsupported branch is gone from the
healthy handler path. Other adr075 guards audited for premise drift at implementation; every retarget
named in the impl evidence.

## Out of Scope (named deferrals)

Counter re-replication / handoff when ring membership changes after committed state (permanent-peer-loss
repair); idempotency TTL pruning (`idempotency_ttl_ms` rides the wire already); counter reset / delete;
decrement / signed counters; CAS / conditional add; multi-key counter transactions; Toron caller-owned
`write_id` exactly-once flush (Toron-home, unblocked by this cut); Toron auto-unlatch.

## Council History

Cross-ring cycle drafted under the ADR 209 thread, relocated here at promotion. The operator's rf=3
table-stakes steer re-priced 075's "when a deployment needs it" deferral and re-ordered the arc (208
guardrail first, this amplifier immediately after, no Toron optimizer work in between — locked in 208's
forward arc). Claude iter-0 decomposed the 075 cut-8b objective into the design questions and found the
durable-postcard witness constraint; Codex iter-1 contributed the typed two-route resolution boundary, the
migration totality rules (v0 `[0]`-only; non-zero index fails closed), and the majority-floor-vs-full-
completion distinction; Claude iter-1 added the third positional assumption (read-path `participants[0]`)
and merged the D-set; Codex iter-2 added primary-routing continuity (`NOT_LEADER` + owner hint) and the
status-constant-stays-for-decoding detail; Gemini iter-3 affirmed the full spine and the relocation.
**One divergence resolved by grounding:** Gemini's final draft described coordinators "seamlessly"
leaving the degraded latch — contradicted by 207's promoted design (the latch is deliberately
process-sticky; its R1 review fix prevents acks from clearing it). D9 carries the grounded 2-of-3:
recovery is a rolling restart, documented, not engineered around.

## Conclusion

ADR 076 finishes what 075 started: fetch-add becomes a real replicated storage-ring primitive —
ring-derived participants, majority durable quorum, peer-identity witnesses that survive ring changes,
full accepted-set completion, total v0 migration — and the fail-closed guard retires from the healthy
path. The day it lands, every rf=3 cluster's Toron coordinators light up 207's durable evidence lane with
a rolling restart and zero code changes.
