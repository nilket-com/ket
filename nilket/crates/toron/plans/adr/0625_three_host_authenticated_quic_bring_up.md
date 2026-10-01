# ADR 625 - three-host authenticated QUIC bring-up over Wi-Fi

**Status:** CLOSED (LIMITED). Successor to ADR 624. The first queries across
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

*Implementation amendments (reviewed).*
- **The probe lives in the process-test binary,** as `#[ignore]` tests
  (`multinode_probe_handshake`, `multinode_probe_query` and
  `multinode_probe_worker_loss`) driven by a spec file
  (`TORON_MULTINODE_SPEC`). It reuses the process fixtures (catalog,
  plans) in place, so nothing is moved. This replaces the planned shared
  module behind `test-support` and keeps fixture code out of the
  library. The fixture storage stays in the worker binary.
- **Every case runs through the scheduler path** (planned, bound,
  fragmented, dispatched), so every attempt records its manifests and
  telemetry.
- **The campaign PKI** is issued by one `#[ignore]` test,
  `multinode_issue_pki`, through the `tls-pki` library: `generate_ca`,
  `issue_listener`, and `sign_client_csr` with an rcgen CSR. It does not
  use the `toron pki` CLI, whose `sign-client` needs an external CSR.
  The issuance functions are the ones the CLI calls.
  - Keys are 0600 and certificates 0644, every file is created new, and
    the SHA-256 of every certificate goes to `fingerprints.json`.
  - Tenant tokens must be positive integers, so the SQL client identity
    is `client-7`.

*Implementation amendments after the runner review (reviewed).*
- **`ListenerPolicy::load_from_paths`.** A new public constructor in
  `network::tls` with the same validation and configs. `load(&TlsConfig)`
  delegates to it, so behaviour is unchanged. The fixture worker uses it,
  so it imports no `config` or daemon types: the ADR 042 and ADR 049
  isolation guards stand unchanged.
- **The handshake proof is an application answer.** The probe dials each
  worker with `connect_peer`, then pulls a partition that cannot exist.
  Only a worker that accepted the client's certificate and served the
  stream can answer `PartitionMissing`. The gate also requires the
  worker's own log to show a `STREAM_ACCEPTED` from the originating
  host's address.
- **A finding: a rejected client certificate leaves no worker log line.**
  The worker's accept loop records neither `HANDSHAKE_REJECTED` nor a
  stream for a client whose certificate it refuses. Checked locally with
  a 3 s wait.
  - Receiver-side evidence is therefore per connection: the rejected
    client's address never appears in a `STREAM_ACCEPTED` line.
  - A valid client right after it is served, so the rejection did not
    wedge the accept loop (checked locally).
  - The side that rejected is read from where the client failed. A
    wrong client's dial succeeds and the connection then ends before any
    answer. A wrong server fails the client's dial with `UnknownIssuer`.
- **Decoupled wrong-CA identities.** `probe-b` and `strix-b` present a
  CA-B certificate while trusting CA-A, so each case is rejected by
  exactly one side. `decoupled_wrong_ca_arrangements_are_rejected_by_the_intended_side`
  checks this locally before any host use.
- **Loss timing.** `dispatch_loop` exposes no internal events, so the
  first error or retry is not observable. Detection timing is reported as
  unavailable, and the outcome is the loop's terminal return within the
  bound:
  - completed with the reference digest (recovered);
  - error within the bound (detection without recovery);
  - completed with another digest (incorrect result);
  - no terminal return within the bound.

  The unannounced bound is **30 s**, as planned. The victim is bound
  before the kill: an accepted sealed manifest from a remote worker,
  identified by task key, attempt and worker. The announced attempt has
  one whole-attempt deadline. The kill command is bounded, and its
  output is kept.
- **The host primitives** are one Python script (`adr625_host.py`), with
  offline fixtures:
  - it starts the worker under `setsid` and verifies that pid, process
    group and session agree;
  - it signals only a group whose every member belongs to the attempt;
  - a failed `ps` or `ss` listing, a malformed record or a foreign
    member refuses;
  - cleanup is verified independently of the record;
  - it checks manifest hashes.
- **The orchestrator and the readers** (`adr625_campaign.py`,
  `adr625_read.py`) have offline fixtures for:
  - the gate, including a failed gate never starting the query;
  - each rejection case, and a timeout not counting as a rejection;
  - query parity and the cross-host edge;
  - forged recovery or replay;
  - cleanup, and `STOP` stopping later attempts;
  - the shared `ACTIVE` lock with the link script.

*Harness amendment after link campaign `c1` (reviewed).*
- **`c1` is kept as a failed attempt,** not deleted or overwritten. Its
  `ping` runs are usable. Its `iperf3` runs (36 of 36) timed out without
  connecting, and its `mtr` runs (6 of 6) exited 1.
  - `iperf3`: each daemon listens on TCP 5201 and is reachable from its
    own host. From every other host, a TCP connection to 5201 did not
    complete within 3 s: the port was **unreachable**. The cause is
    unresolved; there is no specific firewall evidence. `iperf3`'s UDP
    mode needs that TCP control connection too. Any change that makes
    5201 reachable is the user's and is named in the results.
  - The user then added
    `sudo ufw allow from 192.168.0.0/24 to any port 5201 proto tcp` on
    all three hosts. Right after, the same 3 s TCP check connected in
    all six directions: reachability changed after the stated `ufw`
    rule addition. A 2 s `iperf3` UDP smoke run (bigfoot to strix, scratch)
    completed without error.
  - **Campaign `c2`** (kept): TCP, `ping` and `mtr` ran in all six
    directions. `iperf3` UDP ran only toward strix; the 12 runs toward
    bigfoot or optiplex exited 1 with `unable to read from stream
    socket: Resource temporarily unavailable`.
  - `sudo ufw status numbered`, as read by the user afterwards: the
    `5201/udp ALLOW IN 192.168.0.0/24` rule existed on strix only;
    bigfoot and optiplex had the TCP rule only. The 12 failed runs are
    consistent with the missing UDP rules; the interventions support
    that inference but do not establish every earlier timeout's cause.
  - Unresolved: an unprivileged UDP echo on 5201 (scratch, run twice)
    also failed from optiplex to strix, though strix has the rule and
    `iperf3` UDP in that direction worked in `c2`. A UDP echo on 7701
    passed in all six directions in the same session.
  - The user then added the `5201/udp` rule on bigfoot and optiplex and
    reloaded `ufw`; `ufw status numbered` showed both rules on both.
    The same UDP echo on 5201 then passed in all six directions,
    including optiplex to strix, whose earlier failure therefore cleared
    with a change on the other two hosts; its cause is not established.
  - `mtr`: `-i 0.2` needs root (`mtr: non-root users cannot request an
    interval < 1.0 seconds`), and no host has passwordless sudo.
- **The revised path command** is `mtr -r -n -c 40 -i 1 -G 2 <dst>`. It
  keeps the 60 s deadline. It replaces the Part 1 command for `c2` and
  later campaigns. Before `c2`, it was run once per directed pair (six
  runs, scratch output, not evidence): all exited 0 within the deadline,
  and `link_metrics` parsed each one.
- **The UDP reader read the sender's rate.** In `iperf3` 3.20,
  `end.sum.bits_per_second` is the sender's rate, while its loss and
  jitter are the receiver's (one `c3` run: 99,997,360 bit/s sent and
  79,959,145 bit/s received at 19.86% loss). `link_metrics` now reports
  `received_bps`, loss and jitter from `sum_received` and `sent_bps`
  from `sum_sent`, and refuses output without them. A fixture pins it.
  Campaign `c3` (all 48 runs exited 0) is read with the fixed reader.
- **Wi-Fi power saving is on** on all three hosts (`iw ... get
  power_save`). It stays on as a recorded condition. It is a hypothesis
  for the bimodal `ping` RTTs (mostly near 200 ms, sometimes a few ms),
  not an established cause.
- **A UDP echo between hosts on 7701-7703 succeeded in every direction.**
  That shows reachability only. It does not replace the authenticated
  QUIC gates in Part 3, which still run per attempt.

*Campaign amendments after the frozen readout (reviewed).*
- **Link figures.** UDP is reported as offered 100 Mbit/s beside the
  actual sender and receiver rates and loss. These link-load figures do
  not establish QUIC query loss rates.
- **The frozen readout is kept as REFUSED** (schedule `schedule.json`,
  sha256 `7a0bb996…`; `readout-frozen.json`). Its five refusals stand:
  - `th-row-number-1..3` fail the original coverage contract
    ("accepted sealed manifests from at least two hosts"). Each is at
    digest and row parity with the reference, but its plan has exactly
    one stage-0 task, so one producer (70202 on strix). They are not
    reinterpreted as passes.
  - `loss-announced` and `loss-unannounced` are setup failures: the
    agent preflight exited 255 with `Host key verification failed`
    (bigfoot's `known_hosts` had 192.168.0.8 but not 192.168.0.6). No
    kill was sent, so there is no evidence of a recovery failure.
- **The ROW_NUMBER coverage amendment** (this case only, prospective).
  An attempt declared under it must show:
  - digest and row parity with the reference;
  - exactly one dispatched stage-0 task, and exactly one accepted sealed
    manifest, for that task, whose producer published bytes;
  - typed telemetry covering each dispatched task once, with tasks run
    on all three hosts;
  - at least one remote sealed pull from that producer (dial line,
    manifest endpoint and nonzero pulled bytes), with the consumer hosts
    reported.

  The reader applies it only to the attempt ids declared under it in
  `schedule-r2.json`. The original attempts, and the other three cases,
  keep the two-producer-host requirement unchanged. A multi-producer
  window fixture is separate, future coverage.
- **The known_hosts correction.** bigfoot's `~/.ssh/known_hosts` was
  backed up to `known_hosts.bak-adr625-optiplex`. 192.168.0.6's ECDSA,
  ED25519 and RSA host keys were then appended, and only after
  `ssh-keyscan` from bigfoot matched the fingerprints slim already trusts
  for `optiplex` exactly (`SHA256:tpRW/ZAAj…`, `gp0Z66xd…`,
  `XC9C6jqK…`). Host-key verification stays on. The exact preflight
  command then returned 0.
- **`schedule-r2.json`** replaces only five ids:
  - `th-row-number-{1,2,3}` become `th-row-number-{1,2,3}-r2`;
  - `loss-announced` becomes `loss-announced-2`;
  - `loss-unannounced` becomes `loss-unannounced-2`.

  Those five run once. The other seventeen attempts, the references, the
  source, executables, PKI and link campaign `c3` are reused. `r2` is a
  composite evidence set, not a fresh 22-attempt campaign
  (`schedule-r2-mapping.json` maps each replacement to its original and
  reason). The loss bounds are unchanged: 120 s whole-attempt announced,
  30 s after a confirmed kill unannounced. Both schedules and both
  readouts are kept.

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

## Results

**Outcome: LIMITED.** This is composite evidence set `r2`, read out in
`readout-r2.json` with no refusal reasons. The frozen 22-attempt readout
(`readout-frozen.json`) stays REFUSED, as recorded above. Every figure
below is read from these artifacts. No timing here is a speedup claim.

**Provenance.**
- **Retained evidence:** `bigfoot:/home/me/work/toron-evidence/adr625/`.
  - `campaign/evidence/` holds the copied raw evidence of every attempt,
    `links-c1..c3`, the schedules, readouts and freeze record.
  - `campaign/harness/` holds the harness and readers.
  - `campaign/adr625-amendments.diff` is the ADR diff as it stood at the
    closing review.
  - The per-host originals are under each host's
    `~/work/toron-evidence/adr625/attempts/`.
  - `campaign/SHA256SUMS` lists all 1,076 files with full SHA-256 and
    verifies with `sha256sum -c`; its own sha256 is
    `09488cdc65a39e52dd8de78142e4abb06c48228f27f39d43507681fcce0b046a`.
- **Full hashes:**
  - `readout-r2.json`: `3c811f13021d41776188821348ef7c13be7b2e37240e41bb2e221a0a0cd95499`
  - `readout-frozen.json`: `f5e5d5bc332de001499848c9b801b92dce20af00a1c5dbe99ffb23303d9230b2`
  - `freeze-r2.json`: `5f98b6dc981706b527d827402be47ad02837df97d5be83462b2ad93b73181abb`
  - `schedule.json`: `7a0bb996f00f8e8d835e80d21f082c62252cc7252d0312c888960af2ed836927`
  - `schedule-r2.json`: `2778fc538e82d789506d10dc280349939bbf9e12a1f6bd2cf2f2d408f1bd021a`
  - `schedule-r2-mapping.json`: `5dea3cad55760fd96b3f84754c2b24c8588fa1d4c90382123d86e8844894b007`
  - The harness and reader hashes are in `freeze-r2.json` (reader
    `1c6538af…`, campaign `8aa0f70c…`, host `ab922561…`). The r2
    readout's provenance repeats them.
- **Source:** `0adb603be87f26a132ae75b22be9bfe039599c80`, tree
  `fde07beefd02b43e4efa1234f118edfbd87158b6`. This commit's code is
  byte-identical to it; only this record changed.
- **Ladder:** a closing ladder was rerun at that commit, on bigfoot's
  clean checkout (cargo and rustc 1.95.0), with logs retained at
  `bigfoot:/home/me/work/toron-evidence/adr625/ladder-closing/`.
  - **A retention slip:** the logs of the original pre-staging ladder
    were not retained. Its figures are therefore not cited; the retained
    closing ladder replaces it.
  - `cargo test --locked -p toron --no-fail-fast`: 46 test binaries,
    5548 passed, 0 failed, 9 ignored. This includes the process tests
    (64 passed, 4 ignored: the host probes) and the architecture suite.
  - `-p toron-tpch` (with an SSH agent, for the seal fixtures): 10
    binaries, 674 passed, 0 failed, 6 ignored.
  - `cargo fmt -p toron -p toron-tpch --check`: clean.
  - Release builds (`toron_fixture_worker` and the `--release`
    process-test binary): 0 warnings. They reproduced the staged hashes
    exactly: `bbd20acbccbd442a0e892b9777de3f7cb6028186e996387f58c89ac33d53af43`
    and `08092621844baa8d6cdd4dfc0f1fc69ec28e601f5cf1d8743411480d8fcad365`.
  - The debug test build has one dead-code warning, for
    `adr589_plain_write` in `exec/join/streaming.rs`. It comes from ADR
    589; this change does not touch that file.
  - The logs' sha256: `toron-test.log` `8e1d3d05…`, `tpch-test.log`
    `25630bdc…`, `rc.txt` `f4df058d…`.
- **Executables:** the release fixture worker (`bbd20acb…`) and probe
  (`08092621…`) were staged per host. The host manifests were verified
  before r2: bigfoot `c9819c66…` (19 files), strix `f4ef85f8…` (9 files)
  and optiplex `b0426150…` (5 files).

**Timing correlation.** Receiver timestamp correlation was unavailable
(`timing_correlation: unavailable` on every attempt). Positive gates rest
on the application acknowledgement plus the exact client `ip:port` in the
receiver's `STREAM_ACCEPTED`. No cross-host timing claim is made.

**Part 1: links.** Campaign `c3` ran 48 runs, all exiting 0. Figures are
the median and range of 3 runs. UDP was offered at 100 Mbit/s (1,200-byte
datagrams); the actual sender rate is shown because several senders did
not sustain it. Ping is 100 echoes.

| Directed pair | TCP, Mbit/s | UDP sent, Mbit/s | UDP received, Mbit/s | UDP loss % | Ping avg ms (max) |
|---|---|---|---|---|---|
| bigfoot→strix | 92.5 (82.0–93.6) | 100.0 (84.6–100.0) | 99.8 (84.4–99.9) | 0.03 (0.02–0.10) | 107.2 (212.9) |
| bigfoot→optiplex | 71.2 (52.8–76.2) | 96.6 (84.6–100.0) | 80.0 (60.0–84.9) | 19.86 (11.85–27.50) | 85.1 (196.8) |
| strix→bigfoot | 85.3 (83.5–86.8) | 96.4 (93.3–98.1) | 96.0 (93.1–97.9) | 0.02 (0.02–0.12) | 133.1 (211.7) |
| strix→optiplex | 44.9 (42.4–62.6) | 78.1 (70.6–79.1) | 70.4 (66.5–71.7) | 7.83 (5.42–10.69) | 101.7 (209.5) |
| optiplex→bigfoot | 58.0 (50.1–62.8) | 76.9 (76.9–77.4) | 74.5 (73.2–74.6) | 3.01 (2.68–4.49) | 128.4 (207.2) |
| optiplex→strix | 59.2 (59.1–67.5) | 84.9 (71.6–85.1) | 81.3 (70.3–82.6) | 2.54 (1.38–3.45) | 125.3 (219.0) |

- Ping loss was 0% on every pair. `mtr` showed one hop with 0%
  destination loss on every pair.
- Links into optiplex lose the most UDP under the offered load.
- These are `iperf3` offered-load observations, not QUIC query loss
  rates.
- Campaigns `c1` and `c2` are kept as failed attempts (see the
  amendments above).

**Part 3: three-host correctness.** Every three-host attempt passed its
authenticated gates on all three hosts (one origin per host), and
matched its single-host reference digest and rows.

| Case | Contract | Corroborated sealed dials per attempt | Dispatch ms: reference; attempts 1, 2, 3 |
|---|---|---|---|
| hash_join | two producer hosts | 3, 3, 3 | 65; 791, 1455, 800 |
| grouped_aggregate | two producer hosts | 6, 6, 6 | 60; 1113, 638, 395 |
| except_all | two producer hosts | 6, 6, 6 | 46; 225, 1002, 1571 |
| row_number (`-r2`) | single-producer window | 3, 3, 3 | 47; 1638, 196, 1522 |

- **What the count means:** each counted dial is a cross-host dial line
  bound to an accepted sealed manifest's producer and endpoint, by a
  worker that pulled nonzero bytes. The counts are corroborated sealed
  dials, not unique partition deliveries, as in ADR 624.
- **`row_number` under the window contract:** the producer is 70202 on
  strix; the consumers are bigfoot and optiplex; tasks ran on all three
  hosts. The original `th-row-number-1..3` stay failures of the original
  contract.
- **Dispatch times** are context only. The reference runs on one host,
  with no traffic crossing the Wi-Fi.

**Part 4: wrong-certificate rejection.** Each case ended with a valid
follow-up gate.
- `rej-2`: `startup_refused_san_mismatch`. The worker refused its
  wrong-id certificate at startup.
- `rej-3`: `server_certificate_refused_by_client`. Its positive control
  also passed.
- `rej-1` (probe-b) and `rej-4` (client-tenant):
  `client_failed_before_acknowledgement_reason_unestablished` at all
  three endpoints. **A named limitation**: the client failed and no task
  ran, but the evidence does not prove the certificate was the reason.
  These are not counted as passes.

**Part 4: worker loss.** In both runs the victim is 70202 on strix,
bound before the kill to its accepted sealed stage-0 manifest. The
original loss attempts were setup failures with no kill.
- **Announced (`loss-announced-2`): recovered in this attempt.**
  - The kill was confirmed in 308 ms (2 group members).
  - The invalidation names that task. Its replacement (attempt 1) ran
    on 70203 on optiplex and was sealed there.
  - The query completed with the reference digest 711 ms after the
    confirmed kill (whole attempt 1090 ms, bound 120 s).
  - This is one observed attempt, not a general recovery guarantee.
- **Unannounced (`loss-unannounced-2`): a named limitation.**
  - No terminal return or correct completion was observed within 30 s
    after the confirmed kill (`attempt_elapsed_ms` 30826; kill confirmed
    in 598 ms).
  - First internal detection or retry timing is unavailable, because
    `dispatch_loop` exposes no internal events.
  - This is the known gap named in the context.

**What this record establishes.** This is the first authenticated
three-host fixture-query bring-up, over the measured 5 GHz Wi-Fi
configuration (power saving on). Authenticated QUIC (mTLS, worker-bound
SANs) ran all four cases across hosts, at parity with single-host
results.
- Two wrong-certificate arrangements were refused at their named
  points.
- One announced worker loss recovered by replay on a surviving host.
- Two limitations are retained:
  - two rejection reasons remain unestablished;
  - after an unannounced loss, no terminal return was observed within
    30 s.
- **A successor should target the unannounced-loss boundary** before any
  claim of autonomous fault recovery. This closure authorizes no
  timeout or TLS change.
