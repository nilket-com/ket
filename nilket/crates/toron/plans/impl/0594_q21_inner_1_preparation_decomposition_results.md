# ADR 594 - q21's replay-1 preparation on strix: lifecycle decomposition of `runtime_prep_inner_1`: results

Measured 2026-09-16 on strix under ADR 591's coordinate as a transient
user service through the start gate, one instrumented release executable
built on bigfoot from this tree, both drivers' ADR 592/593 plumbing in
force. **The run's off range was 799.1 ms, 8.90% of its 8,980.5 ms
median: above the predeclared 4% cutoff, so the record closes on the
variability finding and no owner is read.** The band did its work on
every pair: the branch facts are established for the first time (q21's
replay-1 preparation at SF=8 under 2 GiB takes the **spilled** branch,
one partition, one partition-sizing pass, 5,845,938 sizing rows,
701,512,560 estimated build bytes), the four terms reconcile with a
residual of 7-23 µs, and the facts are consistent on all five pairs. The
leading term is `inner_1_build_write` at 1,387.5 median-scaled ms, 15.45%
of the off median, reported as leading and not as owner. The journal
places a login-session teardown inside child 4's window and further
session events inside children 5 and 6 — the launching session, ended
by the launching workstation, the same deviation ADR 593 recorded — and
the slow samples are children 6 through 10; the readings do not establish
a cause.

## Identity

| identity | value |
|---|---|
| source tree (instrumented) | `fb46c52224a35fda764f240a5a537b672a496ca8` (= `3d4d598d`, the instrument and driver before this record was folded in; the closing commit differs in the plan documents alone) |
| instrumented executable | `706b234a0a52cc041ecf826fb4a75ae43c9bce9cdeadf903c025deeedb783bae`, both arms of every pair |
| build manifest | `evidence/build/adr594-instrumented.manifest` (bigfoot and strix; staged under `~/work/toron-evidence/staged`, hash verified on strix) |
| recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580` |
| sealed baseline (bound by content) | `950ebc4b…` on tree `a962f3d8…`, artifact `d03935eb…`, q21 median 9,126.9 ms, affinity `0-15` |
| driver unit | `adr594-inner-1.service`, cgroup `/user.slice/user-1002.slice/user@1002.service/app.slice/adr594-inner-1.service`, Unix session id 402000, audit session id 2143, `Linger=yes`; unit settings as ADR 593 recorded (no affinity, no quota, `OOMScoreAdjust` 200) |
| start gate | package 38,000 millic, load 0.19 |
| build host / run host | bigfoot (`~/work/ket-toron`, rustc 1.95.0, clang 21.1.8, mold 2.40.4) / strix |

All eleven rows admitted by ADR 592's validator with complete host
readings (nothing missing). The off median sits 146.4 ms (−1.6%) from the
sealed q21 median and 84.2 ms (−0.9%) from ADR 593's 9,064.7 ms, observed
distances. Evidence on strix: `target/tpch/adr594-q21-inner-1/`
(`samples.jsonl` `a4d24674dca5854706d88851edaa363c1d194787f9db79864a9c9227a9f8f876`,
`summary.json` `47ffc6be0c59963e75f4a59f7e7144e22ea3bd629ed7c00018def65f19cd9694`)
and `target/tpch/adr594-strix-host-record/` (`host_config_before.txt`
`74b43ba9…`, `host_after.txt` `376d672b…`, `driver_stdout.log`
`9b1c0cb3…`, `tool_versions_run_host.txt` `d88e2dc9…`).

## The run

Started 07:25:08 CDT at 38 °C and load 0.19, finished 07:27:23, host at
44 °C and load 0.81 ten seconds later, spill scratch empty. Warmup (off)
8,911.7 ms. Pairs, off / on, movement: pair 0 (off, on) 8,948.2 /
9,021.6, +0.82%; pair 1 (on, off) 8,969.4 / 9,114.8, +1.62%; pair 2
(off, on) 8,980.5 / 9,958.9, +10.89%; pair 3 (on, off) 9,747.3 /
9,848.1, +1.03%; pair 4 (off, on) 9,604.7 / 9,687.0, +0.86%. Off median
**8,980.5 ms**, off range **799.1 ms** (8,948.2 to 9,747.3), on median
9,687.0 ms. Perturbation gate: median `|m_i|` 0.0103, accepted. Every
profile complete; the six identities reconciled on every on row.

**Variability: 0.0890 against the 0.04 cutoff. Exceeded. Closed on the
variability finding; ADR 585's rule is not applied.**

## The band, on every pair

| pair | `runtime_prep_inner_1` µs | sizing scan | partition sizing | build write | residual | residual bound | branch | partitions | passes | est. bytes | sizing rows |
|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|
| 0 | 1,341,551 | 198,896 | 265,297 | 877,344 | 14 | 140,434 | spilled | 1 | 1 | 701,512,560 | 5,845,938 |
| 1 | 1,353,036 | 201,719 | 266,409 | 884,887 | 21 | 213,063 | spilled | 1 | 1 | 701,512,560 | 5,845,938 |
| 2 | 2,209,772 | 200,787 | 267,924 | 1,741,049 | 12 | 1,088,828 | spilled | 1 | 1 | 701,512,560 | 5,845,938 |
| 3 | 2,075,380 | 202,793 | 266,913 | 1,605,667 | 7 | 204,599 | spilled | 1 | 1 | 701,512,560 | 5,845,938 |
| 4 | 1,870,156 | 203,023 | 267,673 | 1,399,437 | 23 | 175,827 | spilled | 1 | 1 | 701,512,560 | 5,845,938 |

Facts consistent on every pair (spilled: `partition_count = 1`, one
sizing-scan call, one partition-sizing call with one pass, one completed
build-write call with 5,845,938 build rows and 27,242,808 spill bytes);
build-write calls 1, residual 7-23 µs against bounds of 140,434 µs and
up. The sizing scan and the partition sizing are steady across the five
pairs (199-203 ms and 265-268 ms); the build write is 877-885 ms on
pairs 0-1 and 1,399-1,741 ms on pairs 2-4, and it alone carries the
term's spread. Under the two steady pairs the term is 1,342-1,353 ms,
beside ADR 593's 1,345.0 ms reading of it.

Median-scaled over the five pairs, reported without an owner:

| term | median-scaled ms | share of off median |
|---|---:|---:|
| `inner_1_build_write` | 1,387.5 | 15.45% |
| `inner_1_partition_sizing` | 263.1 | 2.93% |
| `inner_1_sizing_scan` | 198.5 | 2.21% |
| `inner_1_residual` | 0.0 | 0.00% |

The floor the rule would have applied is `max(500, 2 × 799.1) =
1,598.1 ms`, which no term clears; that is stated for the record and is
not a below-floor outcome, since the cutoff closed the record first.
ADR 585's fifteen-term rule on the same rows returns `runtime_prep_inner_1`
resolved at 1,854.3 ms (20.65%), inflated by the same three pairs, and is
reported as context, not read.

What the facts establish, independent of the variability: at SF=8 under
2 GiB budgets, q21's replay-1 preparation takes the spilled branch. The
preparation therefore reads the filtered `orders` input three times
before the drive reads it again: the sizing scan, one partition-sizing
pass, and the build write, and on the steady pairs those cost about 200,
266, and 880 ms of the 1.34 s term. The candidate ADR 594 named — the
sizing passes a single-pass sizing or a carried estimate would not read —
is priced by these numbers at about 465 ms gross on the steady pairs,
and the build write, not the sizing, is the larger part of the term.
Neither is an owner under this record.

## The readings, in run order

| child | arm | execute ms | temp start→end °C | P-core clocks at start (MHz) | at end | ctxt vol/invol | reading µs | gap ms | journal |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| warmup | off | 8,911.7 | 83→77 | 800–5,900 | 800–5,981 | 11,733/72 | 361/286 | — | 0 |
| 1 | off p0 | 8,948.2 | 85→77 | 800–5,900 | 800–5,700 | 11,732/77 | 375/251 | 7 | 0 |
| 2 | on p0 | 9,021.6 | 88→78 | 800–5,900 | 799–5,779 | 11,733/73 | 245/271 | 7 | 0 |
| 3 | on p1 | 9,114.8 | 89→83 | 800–5,900 | 800–5,489 | 11,732/74 | 376/264 | 6 | 0 |
| 4 | off p1 | 8,969.4 | 91→83 | 800–5,891 | 800–5,813 | 11,731/84 | 394/277 | 6 | 2 |
| 5 | off p2 | 8,980.5 | 91→85 | 800–5,800 | 800–5,886 | 11,733/78 | 253/250 | 6 | 8 |
| 6 | on p2 | 9,958.9 | 90→80 | 800–5,877 | 800–6,000 | 11,733/86 | 404/300 | 19 | 4 |
| 7 | on p3 | 9,848.1 | 88→69 | 800–5,855 | 799–5,841 | 11,732/80 | 372/240 | 41 | 0 |
| 8 | off p3 | 9,747.3 | 90→76 | 800–5,892 | 800–5,468 | 11,732/83 | 248/457 | 38 | 0 |
| 9 | off p4 | 9,604.7 | 90→74 | 800–5,900 | 800–5,773 | 11,732/71 | 395/450 | 15 | 0 |
| 10 | on p4 | 9,687.0 | 90→72 | 800–5,810 | 800–5,628 | 11,731/641 | 465/244 | 17 | 1 |

Package temperatures at the start readings are 83-91 °C and at the end
readings 69-85 °C, the same shape ADR 593 read; one P-core thread at
5.5-6.0 GHz at each endpoint and the rest at 800 MHz; reading cost
245-465 µs. The reading thread's involuntary context switches are 71-86
on ten children and 641 on the last, an outlier on one child with no
journal event but a wireless `UFW BLOCK` in its window; what the extra
switches were is not established. Inter-child gaps are 6-7 ms on the
first five children and 15-41 ms on the last five. The limits ADR 593
stated apply: two instants per child, one thread's counters, no trace
of the seconds between.

The journal, collected once after the last child: `collected`, 15
entries, none malformed, every child's window `complete`. Joined:
the launching SSH session (`session-2205.scope`) logged out at
07:26:01.03, inside child 4's window (off, pair 1), with logind "waiting
for processes to exit"; a session 2206 opened and closed at 07:26:09.7
inside child 5's window (off, pair 2), and a session 2207 opened at
07:26:23.1 inside child 6's window (on, pair 2) and stayed open through
the end of the run; one kernel `UFW BLOCK` on the wireless interface
inside child 10's window. The slow samples are children 6 through 10,
on both arms; the teardown sits two children earlier, in child 4, and
the other session events in children 5 and 6. Those are positions in
the sequence, not an immediate transition, and not a cause.

## Protocol deviation, recorded

The run was launched with `systemd-run --user --wait` from one SSH
session, intended to be the only session for the whole window. The
workstation's harness terminated that SSH client 53 seconds into the run
(the same "low memory" kill that ended ADR 593's polling loops), so
`session-2205` was torn down by logind inside the window; the unit itself
survived because `Linger=yes` holds the user manager. Session 2206 was
my check that the unit was still running, and session 2207 the wait
that replaced the killed one. Three login-session events fell inside the
window, all mine. The run is the record's one run, and the record
closes on it; no second run.

Whether the teardown perturbed the samples is not established. What the
sequence shows is stated as a sequence: the teardown is in child 4's
window, session events in children 5 and 6, and the slow samples are
children 6 through 10 on both arms, while children 4 and 5 themselves
are steady; the build write is the term that carries the spread. The readings show no
thermal or clock change at the endpoints that distinguishes the slow
children. The next launch procedure removes the launcher's session from
the window entirely: `systemd-run --user --on-active=<delay>` schedules
the unit from a session that exits before the unit starts, and the
result is read after the run's expected duration from one session; no
session opens while the unit runs.

## Implementation and verification

Engine: `prepare_reoriented_inner_join_runtime_with_streamed_scan_build_attributed`
takes an optional profiling handle; the chain handler passes the stage
handle for `JoinReplayId(1)` only; under a handle the preparation
records `inner_1_sizing_scan` (one span around the estimate),
`inner_1_partition_sizing` (one span around
`choose_streaming_scan_inner_build_partitioning`, whose retry loop runs
inside it and returns its pass count only under the gate), `inner_1_build_write` (one
span around the spilled branch's create, write, and finish; zero calls
on the partitioned branch), and the `inner_1_facts` diagnostic (branch,
partition and group counts, passes, estimated bytes, sizing rows, call
counts, build rows and spill bytes), or `branch = unsupported` on the
non-streaming fallback; without a handle nothing is read or recorded and
ADR 585's parent clock is untouched. The retry policy is factored into
`streamed_inner_build_partitioning_step`, and the production loop takes
its measurement as a parameter. Receipt: `inner_1_decomposition_band` reconciles the three
children and the residual against `runtime_prep_inner_1`, reports the
facts, and states `inner_1_facts_consistent` with the first
inconsistency named (missing facts, call counts other than one, a
partitioned branch with a build-write call or one partition, a spilled
branch without one partition or one completed write, a pass count below
one), or `branch = unsupported` alone; a partial band or children
exceeding the term are defects; an unarmed profile carries no band.
Harness: the ADR 585 driver generalized to `run_lifecycle_decomposition_driver`
over a spec (label, tags, kind, evaluator, stamps), so ADR 593's scope,
gate, stamps, journal, and cutoff are one path shared by the ADR 585 and
ADR 594 drivers; `adr594-q21-inner-1` with `evaluate_adr594_pairs` over
the band (per-pair terms, facts, identity, consistency, residual bound;
four-term rule with `resolved`, `unresolved_tie_within_off_range`,
`below_floor_no_mechanism`, `withheld_incomplete_profiles` /
`withheld_instrument_incomplete`, `withheld_unsupported_path`, and the
cutoff's `closed_on_variability`) and the ADR 593 references stamped as
context. Tests: the band on the partitioned and spilled fixtures, the
unarmed profile, children exceeding the term, a partial band, nine
named inconsistencies, and the unsupported path; the resolver counting
one pass per measurement with a refused-then-accepted sequence and the
exhausted case; the ungated entry preparing the same partitioned runtime
as the plain one; the evaluator returning every named outcome on
fixtures, the spilled branch naming the build write, and the cutoff
applied after the rule.

## Review round: three contracts tightened, the retained rows re-evaluated

Under review after the run, in the closing commit. First, the pass
counter was not gated: the partition-sizing loop initialized and
incremented it whether or not a handle was present. The loop now counts
only when asked (`count_passes`, which is the handle's presence), and the
production loop itself takes its measurement as a parameter, so the
fixture drives the real loop: a refused first count retried at double
yields one call and two passes, the same loop without the gate measures
identically and counts nothing, an accepted first count yields one pass,
and exhaustion returns the sizing's error after every pass. The
test-only copy of the loop is gone. Second, the partitioned branch's
consistency required zero build-write calls but not zero build-write
time, so a receipt with time on the clock and no call could reconcile
(a correspondingly smaller residual) and call its facts consistent; the
band now requires `inner_1_build_write_micros = 0` on that branch, with
a fixture of 5 µs, zero calls, and residual 5 named as "partitioned
branch with build-write time". Third, the four-term rule reused ADR
586's owner, which marks a leading residual incomplete only above the
floor; ADR 594 says a residual that leads is incomplete at any size, and
`adr594_owner` now says so before the floor is consulted, with a fixture
of a 300,000 µs term whose children are 95,000 µs each, residual
110,000 µs inside its 115,000 µs bound and far below the floor, withheld
as an incomplete instrument.

The retained rows were re-evaluated on strix under the tightened rule by
an `adr594-reevaluate` verb (the evaluator and the cutoff over a
retained journal, warmup excluded as the driver excludes it) built from
the closing code (`c462a254264e51205656c96b7fa0e208d4affb34bf06409296747ec96ec71bc0`,
tree `82ace1c7…`, recipe `9af51cbc…`, manifest
`evidence/build/adr594-reevaluate.manifest`): `closed_on_variability`
with the same leading term and medians, facts consistent on every pair,
branches `["spilled"]`, residuals complete; the new partitioned-branch
check is vacuous on this run's spilled pairs, and no residual led. Report
retained as `adr594-strix-host-record/reevaluation_under_tightened_rule.json`
(`662925ebb8a735c5114dcfe29b48ed948e768aaca022e3541bdc7236f9f4665f`);
`samples.jsonl` and `summary.json` unchanged; the measured executable
`706b234a…` is the run's identity. No performance run was repeated.

Ladder on bigfoot (`RUST_MIN_STACK=67108864`, fmt clean): Toron library
1,809, architecture 2,151, integration 3,649; toron-tpch library 241 (+4
ignored, the ADR 538 atlas-driver test included and passing),
`toron-tpch` binary 108, `toron-bench` 9, integration 261 (+2 ignored).

## Closure

ADR 594 closes on the variability finding: the one run's off range
exceeded the predeclared cutoff, no owner is read, and the instrument,
its facts, and its reconciled bands stand for the record. What it
established without an owner: the spilled branch, one partition, one
sizing pass, three reads of the filtered build input before the drive,
and a build write that is the larger part of the term on the steady
pairs. The successor is the same decomposition launched without any
login-session event in its window (`--on-active` scheduling, one
read-out session after the run), under the same coordinate, gate, and
cutoff; not a second run under this number.
