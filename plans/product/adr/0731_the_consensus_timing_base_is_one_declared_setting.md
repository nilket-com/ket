# ADR 0731: the consensus timing base is one declared setting

Status: accepted as a design, 2026-11-14 (Claude, Codex 01a1d861). Defect DC-237, which this does not close. No value other than today's is adopted. Records: `reviews/raft-obs-20261114/`.

## Why
- The control group's Raft timing is openraft 0.9.24's defaults, taken implicitly: `ControlRetention::openraft_config` (consensus/retention.rs:51) builds `Config { cluster_name, ..Default::default() }` and node.rs:2305 validates it. Nothing sets, reports or documents heartbeat 50 ms, election 150..300 ms, snapshot chunk 200 ms. They are a product timing policy nobody chose.
- Three-host runs on 2026-11-14 (DC-237): after a leader was killed, and in another run after a leader's orderly restart, the cluster did not show a usable leader within the test's bounds. An instrumented run recorded, with node 1 leading, 118 of 2,574 and 524 of 2,584 AppendEntries calls dropped unfinished while waiting for the response, while the followers' handlers mostly finished the same RPCs in under 1 ms. The hosts interconnect over Wi-Fi; one idle ICMP sample had mean round trips of 65, 81 and 126 ms. None of this attributes the failures to the timing values. It does show the fixed values were applied on paths much slower than they presume, and that the operator has no way to declare a different profile.

## What openraft 0.9.24 does with each field (read from the locked source)
| Field | Default | Used for |
|---|---|---|
| `heartbeat_interval` H | 50 ms | the timeout wrapped around every AppendEntries call, heartbeat or payload, connection use included (replication/mod.rs:435); the per-RPC timeout of a leadership-confirmation round in `ensure_linearizable` (core/raft_core.rs:308); when the next heartbeat is due (raft_core.rs:1288); and, times 1.5, the tick period at which heartbeats are sent and election timeouts checked (raft/mod.rs:250) |
| `election_timeout_min` Emin | 150 ms | lower edge of the randomized election timeout; the timeout on each Vote call (raft_core.rs:1066) |
| `election_timeout_max` Emax | 300 ms | upper edge of the election timeout; the leader lease a voter honours before granting a vote to another (engine/engine_config.rs:56); the shorter-log wait is 2 Emax (line 55) |
| `install_snapshot_timeout` | 200 ms | timeout on each snapshot chunk call |

The API takes the four values independently and checks only `Emin < Emax` and `H < Emin`. What the dependency itself couples: heartbeat cadence with the AppendEntries and confirmation-round timeouts (one number, H; the outer timeout is inside openraft, so our network layer cannot lengthen one without the other); the Vote timeout with Emin; the lease and shorter-log wait with Emax. These timeouts are cooperative: when the timed call is polled, the call itself is polled first and a completed value is returned even after the nominal time; only a call still pending is ended by a ready timer. So a call can complete later than its nominal value (one completed AppendEntries wait of 243 ms was recorded under H = 50).

## Decision
1. One setting, `tuning.consensus_timing_base_millis` (B), declares the node's consensus timing profile. One function, the only place that writes openraft's timing fields, derives them:
   `H = B`, `Emin = 3 B`, `Emax = 6 B`, `install_snapshot_timeout = max(200, 4 B)`.
   The ratios are openraft's own defaults. One public scale with fixed ratios is OUR choice, for a setting that cannot express an invalid or lopsided combination; it is not something the dependency forces.
2. Absent, B = 50, and the four fields equal today's values exactly.
3. Accepted range 10..=1000, an integer of milliseconds. Outside it the daemon refuses to start with `tuning.consensus_timing_base_millis must be between 10 and 1000`. No clamping. The derivation uses checked arithmetic and the result still passes openraft's `validate()`; a failure of either is a startup refusal, never a wrapped or substituted value.
4. The node's authenticated readiness answer, which `inspect --json` forwards, gains `consensus_timing: { base_millis, heartbeat_millis, election_min_millis, election_max_millis, snapshot_chunk_millis }`: the declared base beside the four values read back from the configuration the Raft instance was actually built with. This is an additive change to that JSON response and to nothing else. A reader of a readiness body without the field (an older node) is unaffected: `inspect` does not require it. The text output is unchanged.
5. Per-node configuration, documented as to be set alike on every member. A mismatch cannot violate Raft safety; it degrades liveness (the member with the smallest value times out first and disrupts). Each node's own answer shows its own profile; one answer says nothing about the other members, and nothing detects or prevents a mismatch. No replicated copy, no check across members, in this ADR.
6. No constant, ratio or text depends on the number of members.

What the setting is NOT: a guaranteed tolerance for network round trips. H covers a whole call (connection, the peer's storage and processing, scheduling on both sides), and the timeouts are cooperative. The documentation states B as a declared nominal profile with operating assumptions, not a latency guarantee.

## What does not change
openraft's version; pre-vote (absent in this version); every Raft, replication and durable format; the snapshot policy and retention; QUIC idle and keep-alive values; every product timer in the census below; the shipped default profile; any fencing, lease or authority check. The one outward change is the added readiness JSON member of Decision 4.

## Nominal timing by profile
Illustrative arithmetic from the ratios, for a follower that had accepted a leader, with prompt ticks. Not a bound on detecting a failure or on restoring service: electing, replicating and applying take further time, and a node that knows of a longer log waits a further 12 B.

| B | AppendEntries / confirmation-round call timeout | Vote call timeout | follower starts an election after silence of about | plus, if it knows of a longer log |
|---|---|---|---|---|
| 50 (default) | 50 ms | 150 ms | 0.45..0.68 s | 0.6 s |
| 250 | 250 ms | 750 ms | 2.25..3.4 s | 3 s |
| 500 | 500 ms | 1.5 s | 4.5..6.75 s | 6 s |
| 1000 | 1 s | 3 s | 9..13.5 s | 12 s |

A larger B abandons slow calls later and deposes a live leader less readily; it also hears from an idle leader less often and replaces a dead one later. This dependency version offers no profile with both.

## Product timers that meet consensus timing (census, production code at a8e865458)
None derives from B in this ADR and none is loosened. "Fails closed" means the timer's expiry refuses or reports an undetermined outcome and never grants authority. A timer that stays fixed is not thereby independent of consensus latency: where its work needs a leader or a quorum, a slower profile makes it expire or stay unsatisfied more often, and the rows say so.

Quorum-confirmation callers (`ensure_linearizable`; openraft bounds each confirmation call by H and then waits for the applied index without a timeout of its own):
| Site | Outer bound | On failure | With a larger B |
|---|---|---|---|
| decision_proposals.rs:920 `lookup_as_leader` | none locally; a remote caller's `DECISION_RPC_DEADLINE` 5 s | `Indeterminate` (write path: `WRITE_STALLED`) | each per-peer confirmation call's nominal timeout is B, under 5 s for any B in range; the lookup as a whole is NOT bounded by that: its applied-index wait and other work have no local bound, and only the caller's or the remote 5 s deadline ends an attempt. No enclosing timeout is added here |
| decision_proposals.rs:970 `cut_as_leader` | none locally; remote 5 s; callers add `CUT_BOUND` 5 s (reads), `OWNER_OP_ATTEMPT_BOUND` 10 s (sweeper), the caller's deadline | `Indeterminate` → transient refusal / `BACKPRESSURED` / `WRITE_STALLED` | as above |
| control_server.rs:5268, 5298 `accept_join` | none | join refused | join waits longer or is refused; no authority granted |
| node.rs:3723 `establish_root_check_if_forming` | none | retry; at fresh bootstrap a failure fails bootstrap | slower formation at most |
| storage/sweeper.rs:859 `barrier` | none; this path runs only without an owner authority (not ParentDecisions) | barrier error | not on the ParentDecisions path |

Leases and takeover (ADR 0729):
| Timer | Value | Safety on expiry | Liveness with a larger B |
|---|---|---|---|
| range lease TTL, `tuning.data_ring_lease_ttl_millis` (node.rs:1182); renewal every TTL/3 through a replicated command | 3 s; 1 s | expiry and takeover are judged by the replicated reducer against the ring clock (`TAKEOVER_GRACE_MS` 1 s past expiry; successor protection 1..60 s; decisions.rs:133-139), not by wall-clock waits on any node; a lapsed lease refuses work | renewals need a leader. When B makes a leader change last longer than the remaining lease (profiles of about 250 and above against the default 3 s TTL), leases lapse during the change: ranges show not leased, the node is not Ready, and replicas become takeover candidates. Fail-closed and recoverable, but a leader change then also disturbs range authority. STAYS FIXED AND SEPARATE: a separately configured trade to assess together with B, since a longer lease also delays takeover after a real failure. This ADR adopts no lease value and does not claim that changing it is sufficient; expiry fencing and recorded outcomes are untouched. |
| `DECISION_MAINTENANCE_INTERVAL` 500 ms (takeover step, clock probe, certification) | 500 ms | cadence only | none |
| `LEASE_GATE_WAIT` 2 s (write_replication.rs:831) | 2 s | `LEASE_EXPIRED`; not on the ParentDecisions path | none |

Proposal, coordinator and client bounds:
| Timer | Value | On expiry | With a larger B |
|---|---|---|---|
| `DECISION_RPC_DEADLINE` (decision_proposals.rs:290) | 5 s per forwarded propose, lookup, cut | `Indeterminate`: outcome unknown, reported as such | more of these during a leader change that outlasts 5 s (B about 500 and above); recorded outcomes stay authoritative; no retry is turned into a claim |
| local `client_write` in `commit`, `propose_owner_op_as`, lease proposals | no product timeout | error → `NotLeader{hint}` or `Indeterminate` | waits as long as openraft does; bounded only by callers' deadlines. Existing behaviour, made more visible by a slow profile; not changed here |
| no-leader case in propose / lookup / cut | no wait | immediate `NotLeader{None}` | callers see it for longer |
| `CLOCK_ADMISSION_WAIT` 2 s, clock probe 1 s / 2 s / freshness 5 s | — | `ClockUnhealthy`, nothing proposed | the probe's sample comes from a cut, which is quorum work: slower consensus can leave samples stale and the gate refusing. Values and the refusal stay as they are |
| `CUT_BOUND` 5 s; `evaluate_delete` and certification `wait_applied` 5 s; `CATCH_UP` 2 s; `CHILD_WRITE_DEADLINE` 10 s; `CHILD_REPLICATE_DEADLINE` 5 s; `OWNER_OP_ATTEMPT_BOUND` 10 s | fixed | transient refusal, `WRITE_STALLED`, `NotCaughtUp`, no acknowledgement counted, or "outcome ambiguous" | each can expire during a longer leader change; all fail closed |
| table write / delete caller deadline (wire; none by default) | caller's | `WRITE_DEADLINE_EXCEEDED`, INDETERMINATE | unchanged meaning |
| `query_stall_bound_millis` | 250 ms default | `QUERY_STALLED` | bounds execution reads, not the consensus cut; independent |
| client `MUTATION_CANDIDATE_BUDGET` 1 s per node; `DATA_RETRY_BUDGET` 3 attempts; backoffs 20..75 ms | fixed | node avoided; `WRITE_ATTEMPTS_EXHAUSTED` | three attempts with tens of milliseconds between them end well inside a slow profile's leader change: a client without its own retry loop sees exhaustion sooner than a leader exists. Stays fixed; documented; a client-side policy is a separate question |

Startup, transfer, shutdown:
| Timer | Value | On expiry | With a larger B |
|---|---|---|---|
| `wait_for_local_leader` after `initialize` (node.rs:3520) | 5 s | bootstrap fails | a single voter elects at once; not scaled by B |
| `request_join_via_seeds`, `wait_for_join_visibility` | 5 s each | bootstrap fails | joining during a leader change fails sooner than the change ends at large B; the operator retries. Stays fixed |
| `accept_transfer_target` (control_server.rs:5479) | 1 s after an immediate election trigger | "did not become leader for transfer" | Vote timeout is 3 B: above B of about 300 a slow vote can outlast it; fails closed (no transfer). Stays fixed; noted as the first candidate to derive if transfers are needed at slow profiles |
| readiness | no timer; no predicate that requires a leader as such | — | several readiness conditions need consensus to have worked (certified-base validation, root-check delivery, leased ranges, owner operations): slower consensus keeps a node not Ready for longer, as seen in DC-237 observation 2 |
| drain 15 s, storage shutdown 30 s, watchdog 85 s, `raft.shutdown()` unbounded | fixed | abort / unclean exit and recovery at next boot | independent of B |
| clock drift monitor 500 ms offset; eviction 300 s | fixed | abort on drift; epoch advance | samples ride on payload AppendEntries: late delivery of those payloads can affect the freshness and offset tests. The limit and the expiry policy stay unchanged |

Stated limit of the census: it lists the timers found by a directed read of the consensus, decision, lease, coordinator, client and lifecycle code; it is not a proof that no other code assumes a leader within some time.

## Algebra
- Identity: the profile is identified by B alone; the four openraft fields are a pure function of B and are never set from anything else. There is no key narrower than the identity: no code path reads one derived field while another comes from a different source.
- Defaults: `derive(50)` equals `openraft::Config::default()` in all four fields (a test compares them against the dependency, so a dependency default that moves is caught).
- Monotone: each derived field is non-decreasing in B, and `H < Emin < Emax` holds for every B ≥ 1, so every accepted B passes `validate()`.

## Limits, stated
- Making the profile declarable is not a correction of DC-237. Whether any profile restores leadership after a leader is lost on the test fixture is unknown.
- Heartbeat cadence cannot be kept fast while the call timeout is lengthened, in this dependency version. Separating them, or pre-vote, needs dependency work that is not started here.
- For any B there are networks on which replies arrive later than H; a member in that state can keep raising terms while the leader never learns of it.
- Per-node setting: a mismatched cluster is possible and only visible, not prevented.
- The long request-body reads and unwritable replies seen on a restarted node (DC-237 observation 2) are not explained or addressed.

## Acceptance
1. Unit: `derive(50)` equals the dependency's defaults; the table above for 10, 50, 250, 1000; checked arithmetic; every accepted B validates.
2. Config: absent → 50; 9, 1001 and 0 are refused with the stated message; an accepted value reaches the `Config` handed to `Raft::new` (asserted at the construction site's seam, not by re-deriving in the test).
3. The readiness answer carries the effective profile, read from the configuration Raft was built with (asserted for a default node through `inspect --json`, and for a non-default profile at the construction seam).
4. Architecture test: openraft's four timing fields are written in exactly one function.
5. Documentation: `plans/ops/configuration.md` section with the nominal table, the operating assumptions, the lease-TTL trade and the client-retry note; the example configuration carries the key.
6. Real three-daemon test (ci/README "Daemon stacks and the real-process test"): the landed one-host workflow at the default profile, once, recorded. This is a startup-path change.
7. One whole-crate run before delivery.

## Declared-profile diagnostic (separate work order, reviewed before any run)
One execution of the landed three-host happy-path workflow with the existing never-landed observer, at ONE candidate B named in that work order beforehand, with the network declared in the record. It is read descriptively against the 040527Z run (unfinished and failed calls by phase and peer, with denominators; cancellations kept apart from deadline expiries; whether the leader's orderly restart converged), and the workflow's own unchanged assertions decide pass or fail. One run shows what that profile did once on that fixture; it does not establish the cause of earlier failures, and persistent late reads would be a residual hypothesis, not a proven separate defect. Only after that is read: one uninstrumented survivability execution at the same declared profile, if justified. No sweep, no automatic rerun.

## Stop points
Plan review → implementation with acceptance 1..5 → real-daemon run and whole-crate run (6, 7) → delivery ruling. The diagnostic work order is requested separately and is not part of delivering this ADR.
