# ADR 624 - shuffle pulls follow the worker transport

**Status:** CLOSED (results in
`plans/impl/0624_shuffle_pulls_follow_the_worker_transport_results.md`).
Sealed shuffle pulls dial through the worker's transport, by the
producer's identity, via the reviewed `ToronTransport::connect_peer`.
With QUIC-only workers, the hash join, grouped aggregate, `EXCEPT ALL`
and `ROW_NUMBER` match TCP at 2 and 3 workers, and the producer-kill
replay matches. A cross-worker sealed pull is proven per run. With only
the old TCP pull restored, the same QUIC queries fail at the consumer's
sealed pull. The first step toward multinode testing on bigfoot, strix
and optiplex. **A correctness record: one host, no latency claim.**

## Context

Toron has a coordinator and worker runtime with a binary protocol
(`wire.rs`) and a pluggable transport, `ToronTransport`, with `TcpTransport`
and `QuicTransport` implementations. Multi-process tests
(`tests/distributed_process_test.rs`) already run queries with shuffles
and producer-kill replay across separate worker processes, but only on
`127.0.0.1` over plain TCP.

A single query cannot yet complete across hosts. The first blocker is in
the shuffle path:
- **Sealed shuffle pulls are hard-wired to TCP.** `ToronWorker::pull_shuffle_inputs`
  (`server/mod.rs`) opens every pull with
  `tokio::net::TcpStream::connect(partition.endpoint)`. It ignores the
  configured transport and bypasses TLS.
- **A remote daemon has no TCP listener.** `daemon.rs` opens a TCP
  listener only when `task_listener_bind` is loopback. A non-loopback
  bind gets only an authenticated QUIC endpoint (UDP), with mTLS server
  names derived from the worker id (`canonical_daemon_dns_name`).

So any cross-host query that shuffles would dial TCP at a QUIC-only
port. The pull needs no new protocol: the worker's generic
`handle_connection` already serves `PullPartition`. The partition
reference also already carries the producer's `worker_id` and
`endpoint`, which are what `QuicTransport::connect_worker` needs to
choose the loopback or authenticated path and the TLS server name.

Other cross-host gaps are known and **out of scope here** (later
records):
- live shuffle reads only the local partition store, and the scheduler
  does not require a live consumer to sit beside its producer;
- membership is static;
- the fixture worker binds a hard-coded `127.0.0.1`;
- toron-tpch has no remote-cluster mode;
- daemon storage assumes one reachable Wendelon cluster.

## Decision

**1. Dialing a peer by identity.** `ToronTransport` gains
`connect_peer(worker_id, endpoint)`:
- Its default dials `connect(endpoint)`, so `TcpTransport` is unchanged.
- `QuicTransport` implements it with exactly the logic
  `connect_worker` has today: loopback dial, or authenticated dial with
  the server name `canonical_daemon_dns_name(worker_id, cluster_domain)`.
- `QuicTransport::connect_worker` then delegates to
  `connect_peer(&worker.worker_id, &worker.endpoint)`. Its behaviour is
  identical, and the coordinator's dispatch is unchanged.

So a producer reference dials with exactly its own `worker_id` and
`endpoint`. No `WorkerEntry` is fabricated (no invented lease,
capabilities or status), and no registry, scheduling or
consumer-identity state is touched.

**2. The worker pulls through a transport.** `ToronWorker` gains a peer
transport, `Arc<dyn ToronTransport>`, used only to dial producer workers
for sealed pulls:
- It defaults to `TcpTransport` for the existing standalone constructors
  and the fixture worker's TCP mode, so every existing worker and test
  behaves as today.
- `pull_shuffle_inputs` dials through
  `connect_peer(producer.worker_id, producer.endpoint)` from the
  `ProducerPartitionRef` it already holds, instead of
  `TcpStream::connect`.
- The pull protocol, decoding and accounting are unchanged.
- A connection failure propagates as before. There is no TCP fallback,
  no new retry and no new timeout or protocol policy.

**3. The daemon shares its one transport.** Today the daemon's
coordinator already uses `QuicTransport` even on loopback, beside its
loopback TCP listener. There is no coordinator-side TCP policy to
preserve. The daemon constructs its existing transport once: either
`QuicTransport::authenticated(…)` from its TLS policy, or
`QuicTransport::new()` without one. It shares that same `Arc` with both
the coordinator and its worker. The peer transport is never chosen from
the daemon's loopback bind, and an authenticated daemon never gets an
unauthenticated `QuicTransport::new()`.

**4. The fixture worker can serve QUIC only.** `toron_fixture_worker`
gains `--transport {tcp,quic}`, defaulting to `tcp`. In `quic` mode it
binds a loopback QUIC endpoint only, with no TCP listener, and pulls
through `QuicTransport`. So a shuffle completes only if the pull uses
QUIC. The bind address stays loopback in this record.

## Tests (bigfoot ladder; multi-process, one host)

They must show **sealed cross-worker pulls**, not merely a query result
over QUIC coordinator connections, or live and local-store reads.

- **The shape is asserted.** Each QUIC case records nonempty sealed
  producer references that cross distinct worker ids. Its query needs
  several producers, using the existing process fixtures' cases.
  QUIC-only workers are asserted to have no TCP listener at their
  announced endpoint.
- **Reproduction first.** On the current code, the same query, data and
  worker count with QUIC-only workers fail at the consumer's sealed pull
  (the TCP dial). The failure is shown to happen there, not earlier, at
  coordinator readiness or connection setup. The fix must turn exactly
  that case into a pass, with the before and after inputs identical.
- **Digest parity over QUIC.** With QUIC-only fixture workers, a hash
  join, a grouped aggregate and a set-operation or window case give the
  same digests as the TCP run, at 2 and 3 workers. The exact query cases
  are named in the results.
- **Replay over QUIC.** In the producer-kill test (a producer killed
  after sealing, then replayed), a changed attempt and an actual retry
  are confirmed, and the result equals the baseline digest. The test has
  a bounded timeout and cleans up its child processes.
- **Unchanged TCP.** The whole existing `distributed_process_test`
  suite and the architecture suite pass with the default TCP transport.
- **Unit.** A recording transport fixture whose `connect` panics and
  which captures `connect_peer` asserts that the worker dials the
  producer's `worker_id` **and** `endpoint`, so an identity-free dial
  cannot pass. An injected connection failure propagates as an error,
  with no TCP retry and nothing swallowed.
- `QuicTransport::connect_worker` keeps its behaviour through the
  delegation, by the existing transport tests.

## Acceptance

- The code and tests above. On bigfoot: Toron and toron-tpch
  `--no-fail-fast`, fmt clean, and a warning-free release build.
- The results document the exact query cases, the build and executable
  identity, and the failure evidence before and after the fix.
- Plan commit, then one implementation commit with the evidence. No
  measurement or benchmark on strix, and no performance claim.

## Guardrails

- Behaviour stays the same on TCP. The new path is chosen only by
  configuration or flag.
- No protocol, scheduler or storage change.
- TLS policy is unchanged: remote pulls go through the existing
  authenticated `QuicTransport`. This record exercises only the loopback
  QUIC path, which proves the pull uses the transport. It does not prove
  remote mTLS or SAN validation; the successor's PKI and authenticated
  cross-host test remains necessary.
- **The claim is scoped.** This removes the sealed-pull transport
  blocker. It does not show that every cross-host query was impossible
  before, nor that all multinode execution now works. The live-shuffle,
  scheduler and storage constraints stay out of scope.

## Forward (proposed, each its own record)

**Network stance.** The three nodes share Wi-Fi for now, and Toron is
meant to run on modest networks, not only on fast fabrics. So Wi-Fi is a
supported target, not just a stopgap:
- every multinode record reports the link (per-pair throughput, latency
  and jitter, measured beside it) as context;
- timeouts, liveness and replay are tested for tolerance of slow, lossy
  links;
- network-bound results are findings about slow links, not noise.

Wired networking, when it arrives, is an additional configuration, not
a replacement.

0. **Link characterization.** `iperf3` throughput (TCP and UDP),
   round-trip latency and jitter, and loss, per host pair. These are
   measured, not assumed.
1. **Three-host bring-up, fixture data.** The fixture worker binds a
   given address with TLS material from `toron pki`. Workers run on
   bigfoot, strix and optiplex, and a coordinator test client checks
   digest parity with the single-host run.
2. **Cross-host fault handling.** Kill a node's worker mid-query; replay
   and digest parity.
3. **Live shuffle across hosts,** or a scheduler rule pinning live
   consumers beside their producers.
4. **TPC-H across hosts**, at first for correctness, on shared or
   replicated data. Then performance on Wi-Fi, read against the measured
   link. Wired results follow as their own configuration.
