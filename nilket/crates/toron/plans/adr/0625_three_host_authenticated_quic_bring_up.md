# ADR 625 - three-host authenticated QUIC bring-up over Wi-Fi

**Status:** PROPOSED. Successor to ADR 624. The first queries across
physical hosts: bigfoot (192.168.0.2), strix (192.168.0.8) and optiplex
(192.168.0.6), all on Wi-Fi. **A correctness and robustness record.
Wi-Fi is a supported target. Timings are reported as context for the
measured links, and no speedup is claimed.**

## Context

**The hosts, as observed.** All three run kernel 7.0.0 and glibc 2.43,
so executables built on bigfoot run everywhere. All three are on 5 GHz
channel 40 (80 MHz). Their Wi-Fi state is not uniform:

| host | network | signal |
|---|---|---|
| bigfoot | `starlink` | −36 dBm |
| strix | `starlink` | −60 dBm |
| optiplex | `starext` (an extender) | −70 dBm |

The wired ports have no link. `iperf3` 3.20, `ping` and `mtr` are on all
three.

**The firewall.** `ufw` is active, and its rules are not readable
without sudo. The user has opened it for `iperf3`, which runs as a
daemon (`iperf3 --server`, listening on TCP 5201 on all three hosts).
Toron's worker ports are not known to be open.

**What exists:**
- **ADR 624.** Sealed shuffle pulls dial through the worker's transport
  with `connect_peer`.
- **PKI.** `toron pki init-ca` and `issue-listener --worker-id` produce
  PEM certificates (keys 0600, never overwritten). A listener
  certificate carries SAN `toron-<uuid>.<domain>` with both ServerAuth
  and ClientAuth. So one certificate is a worker's server identity and
  also its peer-client identity.
- **mTLS policy.** `ListenerPolicy::load` checks the listener SAN
  against the worker id.
  - The WorkerTask listener requires a client certificate that chains
    to `peer_ca` and has a `toron-*` SAN.
  - A client accepts only the exact
    `canonical_daemon_dns_name(worker_id, domain)` server name for an IP
    endpoint, with no DNS involved.

**Gaps this record closes:**
- The fixture worker serves only loopback QUIC or TCP. It cannot bind a
  host address with TLS material.
- The process-test fixtures (catalog, plans, the fixture storage) live
  inside `tests/distributed_process_test.rs`, so nothing outside the
  test binary can run them against remote workers.
- Nothing has exercised authenticated cross-host QUIC, nor wrong-CA
  rejection on the WorkerTask listener.

**A known robustness gap, measured and bounded here.** Worker loss is
detected only when something calls `mark_worker_lost`; no production
code does. The only automatic signal is QUIC's idle timeout (ADR 577:
300 s, with 5 s keep-alive). `NetworkTaskDispatcher` has no dispatch
timeout. A retryable transport error is retried (`max_attempts` 2) but
never marks the worker lost. So **a worker that disappears may stall a
query.** The 300 s idle timeout is not an established end-to-end upper
bound.

## Decision

The record has four parts, each with its own evidence. Every attempt
writes a new evidence file and never overwrites one: this record keeps
ADR 624's lesson of the overwritten log.

### Part 1: link characterization (no Toron code)

It uses the user's running `iperf3` daemons, one test at a time. The six
directed pairs (A→B is a separate pair from B→A) cover both directions.
Link characterization never runs while a query attempt is running. The
network is otherwise idle. Per directed pair:
- **TCP:** `iperf3 -c <dst> -t 10 -J`, 3 repetitions.
- **UDP:** `iperf3 -c <dst> -u -b 100M -l 1200 -t 10 -J`, 3
  repetitions. Throughput is reported as achieved **under that 100
  Mbit/s offered load** (1,200-byte datagrams, matching QUIC's typical
  datagram size), with jitter and loss.
- **Latency:** `ping -c 100 -i 0.2 -W 2 -w 40 <dst>`, giving min, avg,
  max and mdev RTT, and loss.
- **Path:** `mtr -r -n -c 50 -i 0.2 -G 2 <dst>` with a 60 s deadline.
  Its hop loss is reported with its limit: a router that rate-limits
  ICMP replies shows loss that the path itself may not have.

Every command has a timeout, and each run's raw output is saved under
its own file named for the pair, tool and attempt. The report gives
median and range per directed pair. Asymmetry and the weak optiplex link
are findings, not exclusions. If `iperf3` UDP to 5201 is refused while
TCP passes, that is a named finding, and that pair's UDP figures are
withheld.

**The worker port.** Each host runs one worker, on the fixed **UDP port
7701**, so the firewall needs exactly one rule per host (`ufw allow
7701/udp`). The user applies it with sudo; the change stays explicit, and
the record names it. Outbound dials use ephemeral ports.

### Part 2: the instrument

- **The shared fixtures.** The process tests' fixture catalog, plans
  and fixture storage move from `tests/distributed_process_test.rs` and
  the fixture binary into one library module behind the existing
  `test-support` feature. The tests then import it. This is a move, so
  test behaviour is unchanged.
- **`toron_fixture_worker` gains host mode.** New flags: `--bind
  <ip:port>`, `--advertise <ip:port>`, `--tls-cert`, `--tls-key`,
  `--tls-ca` and `--cluster-domain`.
  - It loads `ListenerPolicy` for its own worker id, so a certificate
    for another worker id is refused at startup.
  - It binds `QuicEndpoint::bind_authenticated(…, server_config_for(WorkerTask))`
    and pulls through
    `QuicTransport::authenticated(policy.client_config(), domain)`.
  - It refuses to advertise a loopback or unspecified address in host
    mode, because a loopback endpoint silently falls back to
    unauthenticated QUIC.
- **A new `toron_multinode_probe` binary** (`test-support`) runs one
  named case against a declared cluster file of worker ids and
  endpoints.
  - It dials with `QuicTransport::authenticated` and its own
    `toron-<uuid>` certificate.
  - Cases: the hash join, the grouped aggregate, `EXCEPT ALL` and
    `ROW_NUMBER` (ADR 624's).
  - It writes one evidence JSON per attempt, containing: the case,
    cluster file, executable and certificate fingerprints, worker
    endpoints, start and end times, the result digest and rows, the
    sealed manifests (producer worker and endpoint), the per-task
    telemetry (worker, pulled and published bytes), the retry count,
    and any error, verbatim.
  - It never overwrites: an existing output path refuses the run.
- **Unit and process fixtures** on bigfoot, before any host run:
  - host-mode argument validation, including the loopback-advertise
    refusal;
  - a certificate for a different worker id is refused at startup;
  - the probe's evidence-writer refuses an existing file.

### Part 3: three-host correctness (authenticated)

- **PKI.** One CA, a listener certificate per worker (bigfoot, strix
  and optiplex) and one for the probe, with domain `cluster.local`. The
  keys are copied only to their own host, and the certificate
  fingerprints are recorded.
- **The reference.** Three workers run on bigfoot at its **LAN
  address**, `192.168.0.2:7701`, `:7702` and `:7703`, with their own
  listener certificates from the same CA. The probe runs on bigfoot too.
  - A non-loopback address takes the same authenticated QUIC path and
    policy as the three-host runs, with no loopback fallback.
  - The reference uses the same query fixtures and authentication policy
    on one host. Its endpoints, identities and host placement are
    recorded. The two configurations are equivalent for the correctness
    comparison, not literally identical.
  - Readiness, firewall accounting and cleanup cover **every port
    actually used** (7701, 7702 and 7703 on bigfoot). LAN-address
    traffic to the local host is not assumed to bypass its firewall. Any
    extra rule the user applies stays explicit.
  - The reference digests are recorded as evidence. The configuration
    used (addresses, certificate fingerprints, trusted roots) is
    recorded with them.
- **The reachability gate.** After each worker reports ready, the probe
  completes the actual authenticated QUIC handshake to it. For the
  three-host runs it does so **from both of the other hosts**, as a
  connection-only check that runs no task. Each check is bound to the
  certificate identity and trusted roots actually used on the host it
  starts from.
  - A failed handshake is reported as **unresolved** among routing,
    firewall, listener and PKI until evidence separates them.
  - It is never labelled "blocked" from a timeout alone.
  - The bring-up stops there; nothing is worked around.
- **The three-host runs.** Workers run on all three hosts (UDP 7701),
  with the probe on bigfoot. Each case runs at 3 workers, three attempts
  each. Every attempt must show:
  - a digest and rows equal to the single-host reference;
  - accepted sealed manifests from at least two hosts;
  - a sealed pull by a worker on one host from a producer on another.
    The edge is read from the manifests, the per-worker dial records
    (ADR 624's recording) and nonzero pulled bytes.
- Timings are recorded beside Part 1's link figures, as context only.
- **If a case shows no sealed cross-host edge** (for example, its plan
  never shuffles across workers), that is reported. The case's coverage
  contract is revised before it runs again; an edge claim is never
  manufactured.

### Part 4: rejection and recovery

**Wrong-certificate rejection.** A second, runnable CA ("CA-B") is
issued explicitly, with a listener certificate for a probe and one for a
worker. Each case is its own attempt and evidence file, and each remote
endpoint is tested **independently**, so a failure on the first endpoint
never stands for the other two. Every case requires:
- an **observed** handshake or startup rejection, with the error
  verbatim. A timeout or a missing result does not count;
- **receiver-side evidence that no task ran**, from the worker's log and
  its dial and task records;
- the trusted roots on each side, recorded.

The cases:
1. **A probe holding a CA-B certificate** dials each of the three
   workers (CA-A roots). Each worker's handshake refuses it.
2. **A worker started with a CA-A certificate issued for another worker
   id** is refused at startup with a `ListenerPolicy` SAN mismatch. It
   never listens.
3. **A worker running with a CA-B certificate** is dialled by the CA-A
   probe. The probe's server verification refuses it at the dial.
4. **A probe holding a `client-<tenant>` certificate from CA-A** (an SQL
   client identity, not `toron-*`) is refused by each WorkerTask
   listener.

**Bounded recovery after a worker disappears.** All of these are
cross-host. A producer worker on another host is killed (`SIGKILL`
over SSH) after its stage has sealed, and the query must complete with
the reference digest:
- **Announced loss**, the existing path: the probe marks the worker
  lost, and the scheduler replays the producer on a surviving host. The
  checks are ADR 624's: a sealed-before-kill manifest, a re-dispatch of
  the exact `TaskKey` at its replacement attempt, and digest parity. Its
  bound is the whole attempt completing within 120 s.
- **Unannounced loss (measure-only).** The worker is killed and nothing
  is told. From the confirmed kill (the process is gone on its host), two
  intervals are measured separately:
  - **detection:** the time to the first observed error, retry or loss
    marking that names the lost worker or its task;
  - **recovery:** the time to completion with the reference digest.
  - **The bound.** The declared target is **30 s**, the value of
    `DEFAULT_WORKER_LOST_AFTER`. That is a target, not proof that the
    dispatcher enforces it. The 30 s criterion **passes only on correct
    completion** within it. An error or retry within 30 s is detection,
    not recovery.
  - **At the attempt's deadline,** the outcome is recorded as one of:
    no response at all; detection without recovery; or an incorrect
    result.
  - The expected failure is a finding, not a reason to widen the bound.
  - No production timeout or liveness mechanism is added in this
    record. A fix is the successor's plan, informed by these observed
    boundaries.
  - `SIGKILL` tests abrupt process disappearance only. It does not
    simulate silent packet loss or a network partition; those are later
    cases.

### The attempt lifecycle (all parts)

This applies to every reference, correctness, rejection and recovery
attempt:
- **A unique attempt directory,** created exclusively (it refuses if it
  exists), locally and on each involved host.
- **Workers are started fresh for each attempt** and stopped afterwards.
  Nothing persists between attempts.
- **Everything is bounded.** The probe has a per-attempt deadline, and
  each remote worker runs under `timeout` with a process-group kill. On
  a deadline or cancellation, the timestamps and events collected so far
  are still written, with the outcome named.
- **Evidence:** the final JSON, plus every raw log: worker stdout and
  stderr, failed startup and handshake output, and the dial records.
  Nothing is overwritten, and every failed attempt is kept.
- **Cleanup is verified on each host before the next attempt.** Only the
  attempt's own recorded PIDs are stopped, never a blanket process kill.
  Then a per-host snapshot must show no process from the attempt, and
  every port the attempt used free. **A failed cleanup stops the campaign.**
- **Key hygiene.** CA private keys stay in the PKI directory on bigfoot
  and are never copied to another host. Each host receives only its own
  worker key (mode 0600) plus the CA certificate. No private key appears
  in any result artifact; only certificate fingerprints are recorded.

## Acceptance

- **The ladder on bigfoot:** Toron and toron-tpch `--no-fail-fast`, fmt
  clean, the architecture suite, and a warning-free release build. The
  same executables are staged and hash-verified on each host.
- **Part 1:** every pair's figures, or a named blocked pair.
- **Part 3:** every case and attempt at digest parity, with a proven
  cross-host sealed edge.
- **Part 4:** the four rejections, each observed at its named point,
  per endpoint, with zero tasks run; announced recovery within its bound;
  unannounced detection and recovery each measured against 30 s and
  reported either way.
- **Evidence:**
  - one file per attempt, under a durable evidence directory on
    bigfoot, with every file's SHA-256 listed in the results;
  - nothing overwritten;
  - every failed attempt kept.
- Plan commit, then one implementation commit with the results.

## Guardrails

- There is no TLS policy change, and no relaxed verifier.
- The worker's listener certificate is its peer-client identity, as
  issued.
- The shared fixture is a move, so test behaviour is unchanged.
- There is no production timeout or loss-detection change in this
  record. Part 4 measures the current behaviour and bounds the attempt,
  not the code.
- Wi-Fi conditions are reported, not filtered. A run disturbed by the
  network is kept and labelled, never discarded.

## Risks

- **`ufw` may block UDP between hosts,** both 7701 for the workers and
  5201 for `iperf3` UDP. The probes find this first. Opening a port needs
  the user (sudo) and is reported, not worked around.
- **The weak optiplex link may make attempts slow or flaky.** That is a
  finding about Toron on this setup.
- **Unannounced loss is expected to exceed 30 s** today. It is reported
  as such.
