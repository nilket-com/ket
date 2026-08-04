# Impl Plan — Wendelon ADR 075: Idempotent Atomic Counter Fetch-Add

> **STATUS: `rf = 1` implemented and verified; `rf > 1` fail-closed.**
> The full producer primitive + client facade (`counter_fetch_add` / `counter_read`) + the 9 D-decisions, 12
> architecture guards and fixtures are complete and verified for replication factor 1 (the default, and any
> single-data-node cluster) — including D4 idempotent replay **across node restarts** (the dedup record is
> durable with the finalization witness / applied log; see code-review F2 in the cut ledger). Under `rf > 1`,
> `COUNTER_ADD_REQ` is **rejected fail-closed**
> (`COUNTER_REPLICATION_UNSUPPORTED`) — an honest narrowed implementation, NOT the full D3 contract. Multi-node
> durable-quorum (`rf > 1`) is **cut-8b**, not yet built. Until cut-8b lands, all plan/ADR/evidence must read
> *`rf = 1` implemented, `rf > 1` fail-closed*.
>
> **cut-8b objective (next session, narrow + explicit):** replace positional accepted-set witnesses with
> **peer-identity** witnesses; make fresh operations **resolve participants from the current ring**; make
> **reconcile target the recorded accepted peers** (not positional indices); **prove `rf > 1`** with
> integration fixtures across ring topology changes; **then remove the fail-closed guard**.

Pairs with `plans/adr/0075_idempotent_counter_fetch_add.md`. Producer-only (no Toron change). The cut adds a
`COUNTER_ADD` data-ring operation on the existing write/finalization path; the two load-bearing pieces are
the **per-key read-modify-write critical section** (D2) and **structural counter/blob type safety** (D7).
All anchors verified.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Generic KV write/read + `write_id` | `Client::write` / `read` (wendelon-client/src/lib.rs:815/1007); `WriteRequestMeta` fixed Pod |
| Per-key primary owner (routing) | `RingState::owner_of_locality` / `owner_of_identity` (network/ring.rs:192/199) |
| Per-key version chain (insertion order) | append-path `DashMap::entry(...).and_modify(insert_version_sorted)` + "different HLCs … cannot interleave" (storage/core.rs:502/581) |
| `write_id` idempotency | `pending_finalizations_by_write_id`, `canonical_idempotency_bytes`, `find_by_write_id` (network/pending_finalizations.rs) |
| Durability / group commit | `wait_durable_commit` / `run_group_commit_driver` (storage/core.rs:319/345) |
| Finalization record | `TenantFinalization` (marker + `charged_bytes`) (network/pending_finalizations.rs) |

## Phase 0 — grounding gates (resolve before locking D2/D7)

1. **Per-key RMW serialization (D2):** confirm whether the storage manager already offers an atomic
   read-compute-append per key, or whether a server-side **keyed mutation lock** must wrap read-latest →
   `checked_add` → append. Version-chain insertion order is NOT sufficient on its own (two adders can share
   `previous`). Land the critical section as `StorageEngine::counter_fetch_add` (preferred) or a keyed lock.
2. **Record kind vs key-domain (D7):** confirm whether the storage record can carry a durable `Blob |
   Counter` discriminant. If yes → typed record (preferred). If no → domain-separated counter keyspace
   (`counter-domain ‖ caller-key`) with **server-side** `key_identity` verification. Caller discipline is
   not an option.
3. **Failed-quorum visibility:** ground whether a locally-appended-but-unfinalized version can be read by a
   later primary read (which would poison `previous_value`). The conditional fixture
   `counter_fetch_add_failed_quorum_does_not_poison_later_previous_value` is required iff this is reachable.

## Phase 1 — wire + storage primitive

- `COUNTER_ADD_REQ`/`RESP` message pair beside `WRITE_REQ` (point writes stay byte-stable, LWW):
  `CounterAddRequestMeta` (write_id, client_hlc, expected_epoch, tenant_id, proxy_hop, durability,
  await_finalization, key_identity, key_len, `delta: u64`) → `CounterAddResponseMeta` (write coordinates +
  `previous_value` + `current_value`).
- `StorageEngine`/`SlabManager` `counter_fetch_add(tenant, key_identity, delta)` implementing the D2 critical
  section: read latest counter version (absent → 0), `checked_add` (overflow → reject, D5), append the new
  LE-`u64` version under the per-key guard. Reject `delta == 0`. Enforce D7 type/domain safety.

## Phase 2 — admission, durability, idempotency

- Route + admit like a point write (tenant lifecycle, quota/charged-bytes, lease gate, durable quorum,
  pending finalization, epoch).
- Typed cached mutation outcome: `CachedMutationOutcome::{ Write(...), CounterAdd { response, trailing } }`,
  keyed by `canonical_idempotency_bytes`; duplicate replay returns the original counter response without
  re-applying (D4).

## Phase 3 — client facade

- High-level `Client::counter_fetch_add(tenant, key, delta, options) -> CounterAddResponse`, mirroring
  `Client::write` by allocating a `ClientWriteId` internally and preserving it across transport retries.
  Lower-level request APIs still expose `CounterAddRequestMeta.write_id` for duplicate-replay fixtures and
  callers that intentionally own idempotency keys.
- `Client::counter_read(tenant, key) -> Option<u64>` (point read + LE-`u64` parse; absent → `None`, D6).
- If domain-separated keys: derive + verify the physical counter-domain key on both client and server.

## Phase 4 — fixtures + guards

The ADR's Integration Fixtures (headline concurrent-sum, racing-adders-don't-share-previous, idempotent
replay, overflow/zero-delta, absent-read, blob/counter cross-reject, leader-change/restart, conditional
failed-quorum) + the 10 architecture guards. The headline + racing-adders fixtures are the D2 proof; the
two cross-reject fixtures are the D7 proof.

## Verification gate

```
cargo check -p wendelon --all-targets
cargo test  -p wendelon counter            # the COUNTER_ADD unit + integration fixtures
cargo test  -p wendelon write              # ordinary point write stays byte-stable / LWW
cargo test  -p wendelon-client counter     # client facade
cargo test  -p wendelon                    # full suite
cargo fmt --all -- --check
```

## Risks

- **D2 critical section is the correctness crux** — without per-key read-compute-append serialization, the
  headline concurrent-sum fixture loses increments. Land it as a storage-manager atomic update or a keyed
  lock; do not rely on version-chain insertion order alone.
- **D7 must be structural** — a typed record kind or a server-verified counter key-domain; never caller
  discipline (it recreates the cross-writer problem).
- **Idempotency outcome typing** — squeezing counter values into `WriteResponseMeta` makes duplicate replay
  guess the shape; use the typed `CachedMutationOutcome`.
- **Charged-bytes growth** — each add appends a version; ground whether compaction of the counter chain is
  needed or the existing version-chain compaction covers it (note for the evidence pass).

## Cut ledger (execution — replication design evolved past Phase 1–3 framing)

The substrate landed as a leader/participant replication design (richer than the original single-primary
sketch): a `CounterLeader` orchestrates a fan-out over a participant set, each participant a replica over the
counter-shaped `SlabManager` primitives.

- **cut-2a/2b/2c** — counter storage primitive (`SlabCounterStore`), the `CounterReplica<S>` tentative/commit/
  rollback logic, `CounterLeader` orchestration (per-key D2 lock, typed D4 idempotency, durable finalization
  witness + reconciler). Landed.
- **cut-3a** — `SlabManager`-backed `CounterReplicaService` (identity-based) + real recovery; the data-server
  node holds one (`data_server.rs` `counter_replica`). Landed.
- **cut-3b** — leader→replica replicate/commit/rollback dispatch + transport (`COUNTER_REPLICATE_REQ` /
  `COUNTER_ROLLBACK_REQ`). Landed **dark** — the remote participant is not yet a `CounterParticipant`.
- **cut-3c** — global `MsgType` opcode uniqueness guard (caught two dark collisions). Landed.
- **cut-3 (local arm)** — `CounterParticipantKind` (production participant enum, `Local`-only — no fake
  remote variant ahead of its transport) + `LocalCounterParticipant` adapter (derives `counter_identity(key)`,
  pins a tenant) + a derivation-locking test driving `CounterLeader<CounterParticipantKind>` over a real
  `CounterReplicaService`. **Staged, under review** (may fold into 3c/1).

### Remaining to the headline (public `COUNTER_ADD` path)

- **cut-4 — `COUNTER_ADD_REQ` server dispatch + `handle_counter_add` (single-node).** **Staged.** Dispatch arm
  mirroring `WRITE_REQ` (bounds-check → parse `CounterAddRequestMeta` → `authorize_actor_for_data(Write)` →
  `try_admit_data_request` → handler). `handle_counter_add` extracts the key from the Arrow payload,
  structurally verifies the counter-domain identity (D7), replays a duplicate `ClientWriteId` from a
  server-side D4 cache (`counter_idempotency`, mirroring the blob-write cache), else drives a lazily-built
  **per-tenant `CounterLeader`** (registry option A) over the node's `counter_replica` (quorum 1, local arm)
  and caches the outcome. Resolved the two cut-4 decisions: registry **A** (no cut-2/cut-3 re-open) and
  server-level idempotency (the leader's `u64` key stays internal — **no widening**). Test:
  `counter_add_dispatch_applies_accumulates_and_replays_idempotently` (apply 0→5, accumulate 5→8, duplicate
  replays 0→5 without re-applying — fresh add still bases at 8). **Deferred to cut-4b** (noted, not silent):
  precise D5 reject status codes (zero-delta vs overflow currently both → `INTERNAL_ERROR`); authoritative
  `commit_hlc` in the response (placeholder zero today); restart recovery via `CounterLeader::recover` +
  `reconcile`.
- **cut-5 — client facade. `counter_fetch_add` half staged.** `counter_identity` is now `pub` in
  `wendelon::storage::counter`. `InnerClient::counter_add` + `counter_add_once` + `decode_counter_add_response_frame`
  + `retry_counter_add_response` mirror the blob write path (route by locality, retry the same `write_id` on
  transient redirect/backpressure). High-level `Client::counter_fetch_add` + `CounterAddReceipt` +
  `Client::counter_identity_for_key`. **Router fix:** `COUNTER_ADD_REQ/RESP` registered in `MsgRouter`'s
  data-ring classification (cut-4 added the `handle_data_frame` arm but not the router classification; the
  dispatch test bypassed the router so it passed — the e2e `client_counter_fetch_add_round_trip_over_hrft`
  caught the gap). **D9 preserved:** counter rejects surface via `ClientError::WriteFailed { status }` (the
  status distinguishes `COUNTER_ZERO_DELTA`/`COUNTER_OVERFLOW`) — no new `ClientError` variant, so Toron's
  exhaustive match is untouched. E2e: `client_counter_fetch_add_round_trip_over_hrft` (apply/accumulate/replay
  through the full client→router→server→leader path). **Still open — `counter_read` wire mechanism:** D1
  defines only `COUNTER_ADD_REQ/RESP`, no `COUNTER_READ`. Committed counters DO land in the manager's
  authoritative `key_index` (cut-3a `…survives_restart_into_manager_index` asserts `mgr.lookup(tenant,
  counter_identity(k))` succeeds), so `counter_read` *may* reuse the normal `READ_REQ` path + LE-`u64` parse —
  but it is unverified whether the read handler returns the counter record's 8 bytes as a readable value.
  Resolve (reuse `READ_REQ` vs. a dedicated `COUNTER_READ_REQ`) before building the read half.
- **cut-5b — `counter_read`. Staged. Fork resolved by grounding:** counters store a framed
  `CounterTentativeFrame` (raw `u64`), but the blob `handle_read` path does `decode_batch(&bytes)` (expects an
  Arrow IPC batch) — so `READ_REQ` **cannot** read a counter. Built a dedicated point-read instead:
  `COUNTER_READ_REQ/RESP` (opcodes 0x4E/0x4F, unique — passes the global MsgType guard), `CounterReadRequestMeta`
  (48B) / `CounterReadResponseMeta` (32B, size-asserted), `handle_counter_read` over `counter_replica.read_committed`
  (absent → `KEY_NOT_FOUND`, D6), router registration, `InnerClient::counter_read` + `Client::counter_read ->
  Option<u64>` (None on absent — never fabricates 0). E2e: `client_counter_read_round_trip_and_absent_over_hrft`
  (read-after-add returns the committed sum; a never-written key returns `KEY_NOT_FOUND`). **Closes the D6
  absent-read fixture.**
- **cut-4b leftovers reassessed (then corrected by code-review F2):** the original deferral said restart
  recovery only matters multi-node — WRONG on two counts (the witness→marker crash window exists single-node,
  and the in-memory dedup loss was missed entirely); `counter_leader_for` now recovers + reconciles (see F2
  below). Authoritative `commit_hlc` stays deferred (minor field; F2 already re-opened `fetch_add`'s return
  type for the load-bearing `Applied`/`Replayed` distinction, but a placeholder-zero HLC misleads nobody).
- **cut-6 — integration fixtures. Partially staged.** **Done:** headline `counter_add_concurrent_unique_writers_sum_exactly`
  (64-way, the D2 proof — verified non-flaky), `counter_add_two_racing_adders_do_not_share_previous`,
  idempotent replay + overflow + zero-delta (cut-4/4b dispatch tests), and the two structural cross-reject
  fixtures (`counter_add_to_blob_key_is_rejected` / `blob_write_to_counter_key_is_rejected`, landed cut-2).
  **Remaining:** absent-read (needs cut-5 `counter_read`), leader-change/restart (single-node restart now
  covered by `counter_add_duplicate_replays_across_server_restart` + the leader-level restart fixtures;
  multi-node leader-CHANGE is cut-8b), conditional failed-quorum (multi-node only — single-node quorum 1
  never fails quorum; the leader-level `QuorumFailed` path is already unit-tested in `counter.rs`).
- **cut-7 — the 10 promotion-locked architecture guards. Staged.** All 10 land in
  `crates/wendelon/tests/architecture_test.rs` as `adr075_*` source-scanning guards (first-class-op,
  per-key RMW lock, ClientWriteId idempotency, duplicate-replays-before-mutation ordering, finalization-
  not-metric, structural blob/counter type safety, no-prefix-scan/registry, blob-write-stays-LWW,
  brand-clean/no-V1, no-Toron-change) + an `adr075_prior_wendelon_guards_continue_to_pass`. Run:
  `scripts/cargo-cgroup test -p wendelon --test architecture_test adr075`.

### Code-review pass (5 findings — ALL FIXED)

A review of the staged cut found four issues, and a follow-up pass on the F2 rework found a fifth; all are
fixed and locked by discriminating tests.

- **F1 (High) — concurrent duplicate `COUNTER_ADD` double-applied. FIXED (subsumed by F2's design).**
  `handle_counter_add` checked a server-side cache then minted a fresh internal `op_id` and applied — a
  check-then-act race: two concurrent requests with the **same** canonical `ClientWriteId` both missed the
  cache, got distinct op-ids, and each added (double-count), violating D4 even at `rf = 1`. Interim fix was a
  server-side per-write-id in-flight gate; the F2 rework then moved D4 wholly into the leader (the finding's
  "pass the public idempotency key into the mutation layer" option), where the per-key D2 lock + under-lock
  recheck serializes concurrent duplicates with no extra gate. Locked by
  `counter_add_concurrent_same_write_id_applies_exactly_once` (32-way same-write_id fan-out → exactly one
  `OK`, rest `DUPLICATE`, a later fresh add bases at one delta, not 32) — fails under the old check-then-act.
- **F2 (High) — restart lost the counter idempotency record; leader recovery wasn't wired to the server
  path. FIXED.** The dedup record was an in-memory server map rebuilt empty on construction, and
  `counter_leader_for` used `CounterLeader::new`, never `recover`+`reconcile` — so a same-`ClientWriteId`
  retry after a node restart re-added against the recovered committed value (D4 → at-least-once on every
  restart), and a crash between the durable witness and the local commit marker left a committed-by-contract
  value invisible forever. (This also corrected the cut-4b-leftovers deferral rationale: single-node quorum-1
  does leave pending witnesses — in the witness→marker crash window — and the in-memory dedup loss was missed
  entirely.) Fix, per the plan's own cut-4 decision ("widen the leader's idempotency key from `u64` to the
  canonical `ClientWriteId`"):
  - `CounterLeader.fetch_add` is keyed by `CounterWriteKey` (`[u8; 24]` canonical `ClientWriteId` bytes; never
    hashed — collisions would break exactly-once) and returns `CounterAddResult::{Applied, Replayed}` so the
    server answers `OK` vs `DUPLICATE` faithfully, including post-restart.
  - The **durable witness carries `write_key` + `previous`** — it is persisted BEFORE the commit fan-out, so
    the dedup record is durable before the value is. On resolution, `drive_commits` compacts the witness into
    an **applied log** (`counter-applied.log`, length-prefixed postcard, torn-tail tolerant) BEFORE pruning the
    witness file: at every crash point exactly one durable artifact holds the dedup → no re-add window at all.
  - `recover` loads applied log + pending witnesses into the idempotency map (a pending witness is an add that
    WILL complete, so it replays even before reconcile); `counter_leader_for` now builds by `recover` +
    `reconcile` under a per-tenant init gate, and `handle_counter_read` also ensures leader recovery so a
    witnessed-but-unmarked add becomes readable after restart. The server-side cache/gate/op-seq were removed
    (the leader's internal op-ids are allocated by its own `op_seq`).
  - Known cost (explicit, not silent): the applied log grows ~44 B per resolved add and is never compacted;
    TTL pruning (the wire's `idempotency_ttl_ms`) is a cut-8b follow-on.
  Locked by: `duplicate_retry_after_restart_replays_from_applied_log` (leader level — fails without the
  applied log), the post-restart-replay assertion in `restart_with_partial_commit_loads_witness_and_reconciles`
  (replays from the recovered witness BEFORE reconcile), and
  `counter_add_duplicate_replays_across_server_restart` (full dispatch across a real node re-bootstrap —
  fails without the `recover` wiring). Guards `adr075_counter_add_reuses_client_write_id_idempotency` /
  `adr075_duplicate_counter_add_replays_without_mutating` retargeted to the mutation-layer shape.
- **F3 (Medium) — counter-read opcodes outside the global uniqueness guard. FIXED.** `COUNTER_READ_REQ/RESP`
  (0x4E/0x4F) were defined + routed but absent from `MsgType::ALL`, so the compile-time `msg_types_globally_unique`
  guard (cut-3c) did not cover them — a future collision on 0x4E/0x4F would not be a build error. Added both to
  `ALL`.
- **F4 (Medium) — replica tentative idempotency race. FIXED.** `replicate_tentative` (both `CounterReplica<S>`
  and the production `CounterReplicaService`) did `contains_key(op_id)` → `await` durable append → `insert`:
  concurrent duplicate replicate frames for one `op_id` could append two live tentatives while commit/rollback
  tracked only the last-inserted location (leaking the first). Was fenced from the public path by the `rf > 1`
  fail-closed guard, but the replica protocol is live (dark). Fix: a per-`op_id` async gate held across the
  check→append→insert in both replicas (mirroring the leader's per-key lock idiom), removed on terminal
  commit/rollback. Locked by `concurrent_duplicate_replicate_appends_one_tentative` (a counting `CounterSlab`
  decorator asserts exactly ONE durable tentative frame under a 2-way same-`op_id` race — the live index is
  last-wins, so the append count is the only discriminating observable).
- **F5 (Medium) — resolved ops didn't seed the recovered op/HLC clock. FIXED.** `AppliedCounterAdd` carried
  only `write_key`/`previous`/`current`, and `recover` seeded `max_op` from pending witnesses alone — so a
  CLEAN restart (no pending witnesses) reset `op_seq` to 1 and the next fresh add re-minted
  `commit_hlc_for(1)`, an HLC a prior committed version already used. Normal latest-reads mask it via
  duplicate-HLC insertion order, but it broke the leader's stated "later ops always order after earlier ones"
  contract (the basis of the stale-reconcile protection and any as-of/version semantics, and a poison risk
  for cut-8b multi-node reconcile ordering). Fix: `AppliedCounterAdd` now carries `op_id`; `recover` seeds
  `op_seq` past the max over BOTH applied records and pending witnesses. Locked by
  `fresh_add_after_restart_gets_strictly_newer_commit_hlc` (add → clean restart → fresh add → the second
  committed version's HLC is strictly newer; fails with an equal/reused HLC without the applied op-id seed).

### Load-bearing fork (cut-4) — needs an operator steer

`CounterLeader` and `LocalCounterParticipant` are **single-tenant** (the adapter pins one `TenantId`; the
leader keys locks/idempotency/witness by `CounterKey` with no tenant). The data server is **multi-tenant**,
and the remote participant arm is still dark, so cut-4's first end-to-end path is necessarily **single-node
(quorum = 1, local arm only)**. Two shapes for holding the leader on the node:

- **(A) Per-tenant leader registry (lean).** `DashMap<TenantId, Arc<CounterLeader<CounterParticipantKind>>>`,
  each leader built lazily with a tenant-pinned `LocalCounterParticipant` + a per-tenant witness subdir.
  **Reuses the landed single-tenant leader/adapter unchanged** (no re-opening cut-2/cut-3 code).
- **(B) Make `CounterLeader` tenant-aware.** Key locks/idempotency/witness by `(TenantId, CounterKey)` and a
  single per-node leader. One leader, but re-opens landed cut-2 code and widens its type surface.

Lean **A** (composes the landed substrate; smaller because the problem is genuinely composable, not because it
avoids re-opening code). Multi-node replication (the remote `CounterParticipantKind` arm + quorum > 1) is the
follow-on once cut-3b's transport surfaces a `CounterParticipant`.

**Second cut-4 decision — leader idempotency key type.** `CounterLeader` keys its D4 idempotency outcome by a
bare `u64` write-id (the internal dark/test path). The public protocol carries a 32-byte `ClientWriteId`;
D4 requires replay keyed by its **canonical idempotency bytes** (`ClientWriteId::canonical_idempotency_bytes()
-> [u8; 24]`; the blob write path keys `IdempotencyKey = ([u8;16], u64)`). So cut-4 must widen the leader's
idempotency key from `u64` to the canonical `ClientWriteId` key — a behavior-completing change that re-opens
cut-2 leader code (it "travels with the ADR"). Hashing `ClientWriteId -> u64` is rejected (collisions break
exactly-once). Note the `op_id` (leader-assigned, drives `commit_hlc`) stays `u64` and distinct from the
idempotency key — only the idempotency map's key widens.

## cut-8 — multi-node D3 replication (quorum > 1) — GROUNDED, needs an architecture steer

The single-node producer contract (cuts 2–7) is complete and verified. The remaining ADR item is D3's
durable-quorum across replicas (and the conditional `failed_quorum_does_not_poison` fixture). It is a
legitimately large, separate replication layer — not a mechanical continuation — and re-opens landed cut-2/cut-4
code. Grounding (all located):

- **Send transport — already built (cut-3b, `#[allow(dead_code)]`):** `replicate_counter_to_remote` (TENTATIVE
  + COMMIT phases) and `rollback_counter_to_remote` over `counter_round_trip` in `data_server.rs`. Server replica
  handlers (`handle_counter_replicate` / `handle_counter_rollback`) also built. So the wire is ready; the gap is
  the **client-side participant** that drives them.

- **Fork 1 — remote-arm layering.** A `RemoteCounterParticipant` must implement `CounterParticipant` (defined in
  `storage/counter.rs`) by calling the send transport (in `network/data_server.rs`). Adding `Remote(...)` to
  `CounterParticipantKind` (in `storage/counter.rs`) would invert the storage→network layering. Options: (A) move
  `CounterParticipantKind` + leader instantiation up to the network layer; (B) a `CounterReplicaTransport` trait
  in storage that network implements, with `RemoteCounterParticipant<T>`; (C) `Box<dyn CounterReplicaTransport>`
  (needs boxed-future trait methods). Lean **B** (keeps storage layer-clean, no dyn-future churn).

- **Fork 2 — leader participant-set model (the crux).** `CounterLeader` holds a **fixed `Vec<P>`** participant
  set, built once per tenant (cut-4 registry, quorum 1, local only). But replicas are **per-key** (per locality
  range from the ring) — different keys have different replica sets. Multi-node therefore needs the participant
  set resolved **per `fetch_add` from the ring**, not a fixed per-tenant Vec. That re-opens cut-2 (`CounterLeader`
  becomes ring-driven) and cut-4 (the registry). Lean: inject a ring/router-backed participant provider into the
  leader; quorum from the replication factor; `read_committed` stays local (participant 0) so remote arms never
  implement it.

- **Then:** multi-node fixtures (`survives_leader_change_or_restart` across two nodes; the conditional
  `failed_quorum_does_not_poison_later_previous_value`) using the `advance_to_two_node_epoch` harness.

Recommendation: this rework of landed cut-2/cut-4 architecture (Fork 2 especially) should be steered before
building — it is the kind of structural change to promoted code that wants an explicit decision, not a
bulldoze. The remote arm (Fork 1, lean B) is self-contained and buildable first once the layering is chosen.

### cut-8a — remote participant arm (Fork-1, lean B) — STAGED + verified

Built: `CounterReplicaTransport` trait (dyn-compatible, boxed futures — keeps storage layer-clean, no
dependency on network) in `storage/counter.rs`; `RemoteCounterParticipant` (a `CounterParticipant` that drives
the replicate/commit/rollback wire via the transport, deriving `counter_identity(key)`, `read_committed`
unreachable since the leader reads its base only locally); `CounterParticipantKind::Remote(...)` variant;
`CounterReplicaNetworkTransport` (network impl wrapping cut-3b's `replicate_counter_to_remote` /
`rollback_counter_to_remote`). Verified: `remote_participant_translates_calls_to_transport` (mock transport
locks the wire translation — TENTATIVE carries value + domain identity, COMMIT carries op_id, ROLLBACK carries
op_id). The leader's quorum > 1 fan-out is already covered by the `TestParticipant` leader tests. The network
bridge is `#[allow(dead_code)]` until cut-8b constructs it.

### cut-8b — production multi-node wiring (Fork-2) — the remaining work, NOT yet built

`handle_counter_add` must build the participant set **per add** as `[Local] + [Remote(peer) for each replica of
the key's locality]` from the ring, and set quorum from the replication factor. The crux is **not** mechanical:
`CounterLeader` holds a fixed `Vec<P>` and its durable finalization **witness stores `accepted: Vec<usize>` —
participant *indices***. Making the set per-key + ring-driven means those indices must stay stable for an op's
whole lifetime *including across a ring-epoch change during reconcile* (a recovered witness re-drives commits by
index; if the ring's replica set shifted, an index could map to the wrong peer). So cut-8b reworks the witness/
reconcile model (index-based → identity/peer-based accepted set) — a correctness-sensitive change to
council-promoted code. This wants a careful design pass (and ideally a steer) before building; bulldozing the
witness rework risks a durability bug. Then: the 2-node integration fixtures (`survives_leader_change_or_restart`,
conditional `failed_quorum_does_not_poison`) over the `advance_to_two_node_epoch` harness, constructing
`CounterReplicaNetworkTransport`.

### cut-8-fence — fail-closed `rf > 1` guard (interim, honest checkpoint)

Per the operator's steer: `default_rf() = 1` is risk-triage, not correctness — `COUNTER_ADD_REQ` is callable under
`rf > 1` (config-supported), where cut-4's single-node leader (quorum 1, local arm) would make the add durable
ONLY on the primary, silently violating D3. So `handle_counter_add` now **fails closed**: if
`self.router.replication_factor() > 1` it rejects with `WriteStatus::COUNTER_REPLICATION_UNSUPPORTED(17)` BEFORE
any mutation. `rf = 1` (the default, and any single-data-node cluster — `replication_factor()` is
`rf.min(data_count)`) is unaffected; all single-node fixtures stay green. Locked by
`adr075_counter_add_fails_closed_under_replication` (status exists + reject precedes the leader mutation). The
behavioral `rf > 1` test lands with cut-8b's multi-node harness. **This narrows the ADR to `rf = 1` until
cut-8b — it is an honest checkpoint, not the full D3 contract.** cut-8b (peer-identity witness, ring-driven
participant resolution, reconcile to recorded accepted peers) remains the real durability deliverable for
`rf > 1`, to be built fresh.
