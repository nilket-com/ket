# Impl Plan — Wendelon ADR 076: Replicated Counter Fetch-Add

Pairs with `plans/adr/0076_replicated_counter_fetch_add.md`. All-Wendelon cut: `src/storage/counter.rs`
(witness + leader rework), `src/network/data_server.rs` (wiring + guard retirement), tests. **No Toron
files; no new wire operations.** Phased so rf=1 behavior is provably unchanged until Phase 4 retires the
reject — the headline fixture flips from red to green on exactly that deletion.

## Anchors (verified)

| Concern | Anchor |
|---|---|
| Index-based witness (positional #1) | `CounterFinalizationWitness.accepted: Vec<usize>` / `committed: HashSet<usize>` counter.rs:1150-1152; postcard `.pw` files :1191-1198 |
| Fixed participants + quorum (positional #2) | `CounterLeader { participants: Vec<P>, quorum, .. }` :1276-1296; `new`/`recover` :1299-1345 |
| Positional read (positional #3) | `read` → `self.participants[0]` :1551-1556 |
| Re-drive loop | `reconcile` → `drive_commits(op_id)` :1544-1549 |
| Per-tenant local-only leader | `counter_leader_for` data_server.rs:7580-7627 (`vec![Local]`, quorum 1, recover + reconcile before first service) |
| rf>1 reject to retire | data_server.rs:7708-7740 (healthy-path branch only; degraded-node fail-closed above it stays) |
| Dark transport to construct | `CounterReplicaNetworkTransport` data_server.rs:288; senders :3862/:3882/:3903 |
| Remote participant trait | `peer: NodeId` shipping fns counter.rs:875-891; `CounterParticipantKind` :991; `CounterReplicaService` :828 |
| Ring discovery + primary | `replicas_for_locality` ring.rs:148/:355 (trait data_server.rs:786); `owner_of_identity` ring.rs:199 |
| Majority quorum rule | `(rf / 2) + 1` data_server.rs:10246 — reuse/share the rule, don't re-derive |
| Two-node epoch helpers | `advance_to_two_node_epoch` tests/client_data_api.rs, tests/data_ring_wire.rs (control-plane/ring shape only; headline replication still needs two live data services) |
| Guard to retarget | `adr075_counter_add_fails_closed_under_replication` (tests/architecture_test.rs) |

## Phase 1 — Flat identity witness (counter.rs; rf=1 net stays green)

- `CounterFinalizationWitness`: `accepted: Vec<NodeId>`, `committed: HashSet<NodeId>`, written directly
  as the durable postcard witness record.
- No `V1`/`V2` type names, no version-prefix envelope, and no legacy positional-witness decoder. This is a
  clean pre-launch format break per `plans/conventions/no-version-suffixes.md` and
  `plans/conventions/no-premature-backward-compat.md`.
- `AppliedCounterAdd` log untouched (identity-free).
- Unit net: identity-witness round-trip through the existing pending/reconcile fixtures; source guards pin
  the absence of version suffixes and migration shims.

## Phase 2 — Leader rework: explicit local arm + two-route resolution (counter.rs)

- `CounterLeader` drops `participants: Vec<P>`/fixed `quorum` for `{ local: L, resolver: R, .. }`;
  the resolver exposes the **two routes that cannot be cross-called** (D3):
  `fresh_participants(key_identity) -> (Vec<(NodeId, P)>, quorum)` (current ring) and
  `participant_for_recorded(node_id) -> Option<P>` (recorded identities only). `drive_commits` can reach
  only the second; `add` only the first. Concrete shape: follow the `CounterParticipantKind` enum pattern
  (the implementing session picks enum-dispatch vs trait object — either satisfies D3).
- `add`: per-key lock (unchanged 075-D2) → fresh resolution → tentative fan-out (local + remotes) →
  majority-quorum check → **identity witness** durable → commit fan-out over the FULL accepted set (D5) →
  applied-log + prune (lifecycle unchanged).
- `drive_commits(op_id)`: recorded `NodeId`s minus `committed`, resolved via the recorded route; an
  unresolvable/unreachable peer leaves the witness pending for the next reconcile pass (bounded backoff,
  D7). Never consults the current replica list.
- `read`: `self.local.read_committed(key)` — positional #3 retired.
- `recover`: semantics unchanged (idempotency from applied + pending; op-seq past both).
- The existing `TestParticipant` leader net retargets mechanically (construction shape) with assertions
  un-weakened — it is the rf-agnostic quorum/rollback/no-poison/idempotency net.

## Phase 3 — data_server wiring (rf>1 capable; reject still standing)

- **Verify-first:** confirm the replica-side `COUNTER_REPLICATE_REQ`/`COUNTER_ROLLBACK_REQ` handlers are
  fully live (they predate this cut); a stubbed handler is a finding to surface, not silently build.
- Construct `CounterReplicaNetworkTransport` (un-dead-code :3862+) as the remote participant transport.
- `counter_leader_for`: stays per-tenant; builds the resolver over `self.router` — fresh route from
  `replicas_for_locality(locality_of(counter_identity))`, self as the explicit local arm, remotes as
  `Remote(peer)`; quorum = the majority floor (share the :10246 rule via a helper, or pin equality with a
  cross-reference test).
- Witness store opens without a local-node migration parameter.
- Add counter handler primary enforcement before mutation: derive the current replica list, require
  the first replica's `NodeId` to equal `self.local_node`, and return `NOT_LEADER` with the first replica as
  the hint otherwise. The current 075 counter path does not have this gate; do not describe it as unchanged.
- Adjust counter-add response construction for redirects: `counter_add_response` currently hardcodes
  `leader_hint: self.local_node`, so the new owner gate needs an owner-hint override or a small redirect
  response helper. Do not return `NOT_LEADER` with the rejecting node as the hint.

## Phase 4 — Retire the fail-closed branch (the cut's last production change)

Delete the `replication_factor() > 1` reject (data_server.rs:7708-7740). The degraded-node branch stays.
The status constant stays for wire decoding (D8). Ordered strictly after Phase 5's fixtures exist —
red-gate discipline: the headline fixture must be observed failing on main and flipping green on this
deletion.

## Phase 5 — Fixtures (two-node harness)

| test | locks | fails under |
|---|---|---|
| `replicated_counter_fetch_add_concurrent_adds_sum_exactly` (rf=2) | **headline** — durable-quorum adds + restart survival + duplicate replay | `main` (reject fires) |
| `reconcile_after_ring_epoch_shift_targets_recorded_peers` | D2/D3 — identity re-drive | positional re-drive |
| `fresh_add_uses_new_ring_while_redrive_uses_recorded` | the D3 boundary, both routes in one fixture | a shared resolution route |
| `failed_quorum_rolls_back_all_accepted_rf2` | no-poison, now unconditional | tentative leak |
| `commit_fanout_completes_beyond_quorum_rf3` | D5 completion ≠ quorum | quorum-set-only fan-out |
| `no_primary_only_success_under_replication` | the 075 guard's purpose, pinned positively | primary-only durability |
| `witness_format_is_flattened_and_identity_only` | D2/D6 convention guard | version suffix / compat shim drift |

Plus the Phase 1/2 unit nets. The headline and remote-path tests must use two live data services over
simulated transport or an equivalent multi-service loopback; a single-node harness with only a synthetic
two-node epoch is insufficient proof that `CounterReplicaNetworkTransport` is constructed and used. Flake
discipline: rerun an isolated failure 3-5× before reporting; the binary-level result is canonical.

## Phase 6 — Architecture guards

The eleven `adr076_*` guards from the ADR, in Wendelon's architecture test home. Retarget
`adr075_counter_add_fails_closed_under_replication` to the positive invariant (purpose preserved,
mechanism inverted — named in the ADR). Audit remaining adr075 guards for premise drift (witness/
participant shape scans); name every retarget in the implementation evidence.

## Verification gate

```text
scripts/cargo-cgroup check -p wendelon --all-targets
scripts/cargo-cgroup test  -p wendelon --lib counter
scripts/cargo-cgroup test  -p wendelon --test simulator_transport replicated_counter
scripts/cargo-cgroup test  -p wendelon --test client_data_api counter
scripts/cargo-cgroup test  -p wendelon --test data_ring_wire counter
scripts/cargo-cgroup test  -p wendelon --test architecture_test adr076
scripts/cargo-cgroup test  -p wendelon --test architecture_test adr075
scripts/cargo-cgroup test  -p wendelon --test architecture_test          # FULL binary (207/208 lesson)
scripts/cargo-cgroup test  -p wendelon --lib
cargo fmt -p wendelon -- --check
```

(`--test` takes the file stem — verify Wendelon's actual test-target names before running; the
architecture-test stem trap has bitten before on the Toron side.)

## Hygiene

- No m-word in src/tests; counter values "strictly increase".
- The witness shape is this cut's ONE deliberate durable-format change: flat postcard with peer identities,
  no version suffixes and no pre-deployment compatibility decoder.
- No Toron files (guard-pinned); no new wire MsgTypes (guard-pinned); all cargo via
  `scripts/cargo-cgroup`.

## Open for the implementing session

1. Replica-side replicate/rollback handler liveness (Phase 3 verify-first).
2. Resolver concrete shape (enum-pattern vs trait object — either satisfies D3).
3. Whether a shared majority-floor helper exists for the :10246 rule or equality is pinned by test.
