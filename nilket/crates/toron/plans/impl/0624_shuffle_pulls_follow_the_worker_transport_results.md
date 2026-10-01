# ADR 624 results - shuffle pulls follow the worker transport

**Done, on one host.** Sealed shuffle pulls now dial through the worker's
configured transport, using the producer's own worker id and endpoint,
instead of a hard-coded `TcpStream::connect`.

With QUIC-only fixture workers (no TCP listener), the same queries now
match the TCP results at 2 and 3 workers:
- a hash join, where the edge proof shows sealed pulls crossing workers;
- a grouped aggregate;
- `EXCEPT ALL`;
- `ROW_NUMBER`.

A producer-kill replay over QUIC also matches.

On the same tree with only the pull reverted to TCP, the four QUIC query
tests fail with `Connection refused` at the consumer's sealed pull.

**Scope.** This removes the sealed-pull transport blocker, and nothing
more:
- it does not show that every cross-host query was impossible before;
- it does not show that multinode execution now works end to end;
- loopback QUIC proves the pull goes through the transport, but it does
  not test remote mTLS or SAN validation, or Wi-Fi behaviour. Those
  belong to the successor's authenticated cross-host bring-up;
- live shuffle, the scheduler and storage constraints are unchanged and
  out of scope.

No strix measurement; no performance claim.

## Identity

| identity | value |
|---|---|
| source | branch `adr624-impl` `50413e50`, tree `c4d38320668c3b6d8442fa1d8bfff95ab1667184`. The closing commit carries exactly this code |
| ladder (bigfoot, `--no-fail-fast`, final tree) | fmt clean; toron 46 suites, **5,543 passed, 0 failed**, 5 ignored (`distributed_process_test` 59/0); toron-tpch 674 passed, 0 failed, 6 ignored |
| release build | 0 warnings; executable `d79045edc6ad07ad02d13cd3e1985f28402c7ecaf556f5df9044ba2a73a50d38`, manifest `adr624-impl.manifest` (`4ec7c735bd710902e65f4eeceebba96a010ea0a20353ef829f1f685892357e0b`). Not staged on strix: there is no measurement |

**Ladder history (kept as provenance).** The first full ladder, on
`4927d576` (tree `5ac59225…`), was **not green**: toron had 44 suites ok
and 2 not ok, with 5,540 passed. Three failures:
- **`architecture::network_tls::adr044_daemon_selects_authenticated_quic_only_for_non_loopback_tls_listeners`**
  and **`adr044_remote_static_worker_requires_tls_and_san_binding`.**
  - Both are source-shape guards requiring the literal
    `canonical_daemon_dns_name(&worker.worker_id, cluster_domain)` in
    `transport.rs`. Moving the body into `connect_peer` changed it to
    `canonical_daemon_dns_name(worker_id, cluster_domain)`. The
    architecture suite had not been rerun after the transport change.
  - **Reviewed amendment:** both guards now require the new SAN form
    **and** `self.connect_peer(&worker.worker_id, &worker.endpoint)`. The
    second also requires `async fn connect_peer(` and the server's
    `.connect_peer(&partition.worker_id, &partition.endpoint)`. The old
    SAN-expression assertion is replaced for the moved body; every other
    original assertion is kept. No guard is removed, and the transport
    policy is unchanged.
  - These remain source-shape checks. They do not replace the
    successor's authenticated cross-host tests.
- **`observability_listener_test::observability_metrics_method_not_allowed_emits_denied_access_audit`:**
  `start daemon: Io(AddrInUse)`. It touches no code in this change. It
  passed 3 of 3 in isolation (36/0 each), and it passed in the final full
  ladder. That is consistent with an intermittent port collision, but its
  cause is **not established**.

The guard amendment changed only `tests/architecture/network_tls.rs`. The
final tree's production code equals the first tree's, and the release
executable is byte-identical (`d79045ed…` both times).

## The change

- **`ToronTransport::connect_peer(worker_id, endpoint)`.** This is the
  reviewed API adjustment: a producer reference dials with exactly its
  own identity, and no `WorkerEntry` is fabricated (no invented lease,
  capabilities or status).
  - Its default dials `connect(endpoint)`, so `TcpTransport` behaves as
    before.
  - The `Arc` forwarding impl forwards it.
  - The default `connect_worker` delegates to it.
  - `QuicTransport::connect_peer` carries the former `connect_worker`
    body verbatim: loopback dial, or authenticated dial with the server
    name `canonical_daemon_dns_name(worker_id, cluster_domain)`.
    `QuicTransport::connect_worker` delegates to it, so the coordinator's
    dispatch is unchanged.
- **`ToronWorker`** gains `peer_transport: Arc<dyn ToronTransport>`.
  - It is `TcpTransport` by default in
    `with_partition_store_and_endpoint`, which every standalone
    constructor goes through. `with_peer_transport` overrides it.
  - `pull_shuffle_inputs` calls
    `peer_transport.connect_peer(&partition.worker_id, &partition.endpoint)`.
  - The pull framing, attempt validation, decoding and accounting are
    unchanged. A dial failure propagates, with no fallback and no retry.
- **The daemon** builds its existing transport once, before the worker:
  `QuicTransport::authenticated(…)` from its TLS policy, or
  `QuicTransport::new()` without one. It shares that one `Arc` with the
  worker and the coordinator. It is never chosen from the loopback bind.
- **`toron_fixture_worker`:**
  - `--transport {tcp,quic}`, defaulting to `tcp`, which behaves as
    before. The `quic` mode binds `QuicEndpoint::bind_loopback` only,
    with no TCP listener. It accepts the way the daemon does (connection,
    then `accept_stream_on`, then `handle_connection`), and pulls through
    `QuicTransport`.
  - A test-only `--record-peer-dials <file>` wraps the peer transport. It
    appends one `<this worker> <producer worker> <producer endpoint>`
    line per dial, then delegates unchanged.

## Tests

**New: 7 in `tests/distributed_process_test.rs` and 1 server unit test.**

| test | what it shows |
|---|---|
| `quic_only_fixture_workers_have_no_tcp_listener` (bounded) | a QUIC-only worker refuses TCP at its announced endpoint; a TCP worker accepts (control) |
| `fixture_worker_that_cannot_start_exits_without_readiness` (bounded) | a worker given an invalid `--transport` exits non-zero without announcing `READY`. It is spawned `kill_on_drop` and explicitly waited |
| `sealed_shuffle_pulls_cross_quic_only_workers_with_tcp_digest_parity` | hash join at 2 and 3 workers: QUIC digest equals TCP digest, 3 rows, and the edge proof below holds on both transports |
| `ordered_grouped_aggregate_over_quic_only_workers_matches_tcp` | 2 and 3 workers: 5 rows, digest parity, all selected workers used |
| `set_op_and_window_over_quic_only_workers_match_tcp` | `EXCEPT ALL` → `[NULL, 7]` and `ROW_NUMBER` → `[(NULL,1),(NULL,2),(7,1),(9,1)]`, at 2 and 3 workers, QUIC equal to TCP |
| `hash_join_producer_kill_over_quic_only_workers_replays_and_matches_baseline` | the producer killed at stage index 0 and at 1 over QUIC; replay digest equals the TCP baseline |
| `cross_worker_sealed_pull_proof_refuses_self_pulls_and_unsealed_producers` (`#[test]`) | the edge proof refuses self-pulls only, an endpoint with no sealed manifest, and a puller with no pulled bytes |
| `server::tests::sealed_pull_dials_the_producer_identity_through_the_peer_transport` (unit) | a recording transport whose `connect` panics sees exactly one `connect_peer` to producer `(WorkerId 42, 192.0.2.42:7000)`, and the injected dial failure propagates unchanged, with no retry |

**The edge proof.** A run counts a cross-worker sealed dial when all of
these hold in the same successful scheduled query:
- the worker's dial record names a producer worker different from
  itself;
- that producer's worker id and endpoint match an accepted sealed
  manifest;
- the dialling worker's tasks pulled a nonzero number of sealed bytes.

Observed: **2 such dials at 2 workers and 3 at 3 workers**, on both TCP
and QUIC. These are dials, not a count of unique partition deliveries.
The claim is that at least one cross-worker sealed pull was used by the
successful query.

**Replay binding** (the TCP and QUIC kill tests both use the shared
runner):
- the invalidated producer's manifest, keyed by its `TaskKey`, was
  accepted before the kill, at `invalidated_attempt` on the killed
  worker;
- the replacement run's `dispatched` contains that exact `TaskKey` at
  `replacement_attempt`.

**Process cleanup.**
- Fixture children are spawned `kill_on_drop(true)`, which requests
  termination when an unguarded `Child` is dropped.
- Readiness is bounded at 30 s, and explicit `shutdown` and `wait` reap
  the normal cases.
- The invalid-transport test proves early failure, no `READY` and an
  explicit wait. It does not prove cancellation of a hung readiness
  read, or synchronous reaping on drop.

**Existing tests.** The process suite chooses its transport through a
task-local that defaults to TCP, so every pre-existing test runs exactly
as before. The one exception is the shared producer-kill runner, which
gained the replay-binding assertions above.

## The reproduction

`adr624-repro-v2.txt` (bridge evidence) records tree `5ac59225…`, whose
production and process-test code equals the final tree's (the final tree
changed only the architecture guard), with **only** the pull reverted to
`tokio::net::TcpStream::connect(partition.endpoint)`. The tree was
restored afterwards, dirty 0.

| test | reverted pull | this record |
|---|---|---|
| hash join parity | FAILED: `TaskFailed { StageId(2), TaskId(1) … "query IO error: Connection refused (os error 111)" }` | ok |
| grouped aggregate | FAILED: `Connection refused (os error 111)` | ok |
| set-op and window | FAILED: `Connection refused (os error 111)` | ok |
| QUIC producer kill | FAILED at the replacement dispatch: `StageId(2), TaskId(0) … Connection refused` | ok |
| no TCP listener | ok | ok |
| unstartable worker | ok | ok |

The producer stages had already run over QUIC on the reverted tree. The
consumer stage then failed dialling TCP at a QUIC-only port, so the
failure is the sealed pull, not coordinator readiness or setup.

## Provenance

The durable evidence is on bigfoot in `~/work/toron-evidence/adr624/`:

| file | sha256 | content |
|---|---|---|
| `adr624-repro-v2.txt` | `de49a27d456f13b8d89f63e1ef9b01cd7c2a79e3e62ab5daf8e3bec8ebc2b4ce` | the reproduction on tree `5ac59225…`, with only the pull reverted; the tree was restored afterwards, dirty 0 |
| `adr624-repro.txt` | `5aed0d749fd9d2d3d354855f9907c7034da77a841b2d60718c9c7c6481d8a1ae` | the first reproduction, on the earlier test set (superseded by v2) |
| `ladder-toron.log` | `ba6f2357928d294e8762a7f31be8fb3a8a120121ba5b7425ad3dc31c249bff4d` | the final toron ladder, tree `c4d38320…` |
| `ladder-tpch.log` | `cbdcc440b01c9a7a9f87d2705b9b6e6639e2dd715fa379ff39ca5223bcdca1e0` | the final toron-tpch ladder |
| `ladder-summary.txt` | `ca985defcb29ed5bccc330e6a9c8b5894564ba2ef587cee12220c65ade10434e` | the final ladder and build summary: tree, fmt, counts, release warnings, executable, manifest hash |
| `adr624-impl.manifest` | `4ec7c735bd710902e65f4eeceebba96a010ea0a20353ef829f1f685892357e0b` | the release build manifest; executable `d79045ed…` is in bigfoot's staging |

**What is missing:** the first (non-green) ladder's full log was
overwritten by the rerun at the same `/tmp` path. That run's three
failures and their messages are recorded above, as read at the time, but
the raw log was not retained.

## Forward

The successor is the three-host bring-up on bigfoot, strix and optiplex:
fixture workers bound to host addresses with TLS material from
`toron pki`, authenticated QUIC with SAN checks, and digest parity with
the single-host run. Every record from there on reports the per-pair
Wi-Fi link.
