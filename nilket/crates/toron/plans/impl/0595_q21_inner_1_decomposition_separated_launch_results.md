# ADR 595 - q21's replay-1 preparation on strix under a verifiably separated launch: results

Measured 2026-09-16 on strix under ADR 591's coordinate, ADR 594's band
and rule, and this record's launch: scheduled from one SSH session that
exited two minutes before the unit started, completion delivered to the
workstation as a datagram, one read-out session afterwards. **The run's
off range was 45.6 ms, 0.51% of its 9,005.8 ms median, inside the 4%
cutoff; the measurement window carried no login-session event; the
launcher's logout lies 123.3 s before the first child; and ADR 594's
rule, applied on those terms, resolves the envelope's owner:
`inner_1_build_write`, 876.4 median-scaled ms, 9.73% of the off median,
613.2 ms ahead of `inner_1_partition_sizing` against a 45.6 ms range and
a 500 ms floor.** One thing stands between the run and that reading and
is stated first: the retained summary withheld the owner as
`withheld_launch_separation_failed`, because the driver's verdict code
compared the launcher's boot id in the kernel's dashed spelling against
the journal's undashed one and found no match. The logout entry is in
the collection on the same boot; the comparison was the defect. The
defect is fixed in the closing commit with a dashed-versus-undashed
fixture, and the retained evidence re-evaluated under the fix returns
`verified` and the resolved owner. Both verdicts are recorded here; the
retained summary is not rewritten.

## Identity

| identity | value |
|---|---|
| source tree (instrumented) | `11fbd3053aa9bf7d541ce27d881a6d6cab713c08` (= `dff4b279`, the launch plumbing before this record was folded in; the closing commit differs in the plan documents and the boot-id fix) |
| instrumented executable | `af10f4a11c0692eadb95b0d41e47104855e61d4f7b19725818decf63158f2a30`, both arms of every pair |
| build manifest | `evidence/build/adr595-instrumented.manifest` (bigfoot and strix; staged under `~/work/toron-evidence/staged`, hash verified on strix) |
| recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580` |
| sealed baseline (bound by content) | `950ebc4b…` on tree `a962f3d8…`, artifact `d03935eb…`, q21 median 9,126.9 ms, affinity `0-15` |
| driver unit | `adr595-inner-1.service` under `user@1002.service/app.slice`, Unix session id 405065, audit session id 2143, `Linger=yes`; `Type=exec`, `RuntimeMaxSec=10min`, `TimeoutStopSec=30s`, `KillMode=control-group`, `KillSignal=15`, `FinalKillSignal=9`; timer `OnActiveSec=120`, `AccuracySec=1s` |
| start gate | package 44,000 millic, load 0.00 |
| build host / run host | bigfoot / strix |

## The launch, verified

| instant (CDT) | unix ms | what |
|---|---:|---|
| 09:49:12 | — | host snapshot, launch session 2233 (load 0.02, package 44 °C) |
| 09:49:13.058 | 1789570153058 | `ADR595_LAUNCH_UNIX_MS`, taken before `systemd-run` |
| 09:49:13.071 | 1789570153071 | `systemd-run` returned (timer started; `NextElapseUSecRealtime` empty for a monotonic `OnActive` timer, the 120 s delay recorded) |
| 09:49:13.101 | 1789570153101 | session 2233 logged out (journal, boot `882b9409…`) |
| 09:51:13 | 1789570273000 | timer trigger as `LastTriggerUSec` reports it, at second resolution: 58 ms before launch + delay against that rounded value, which bounds the true trigger to within a second |
| 09:51:13.964 | 1789570273964 | driver start; 964 ms after the rounded trigger, so between 0 and about 1 s after the true one; gate read: 44 °C, load 0.00 |
| 09:51:16.415 | 1789570276415 | first child's launch (the warmup) |
| 09:53:23.358 | 1789570403358 | last child's return |
| 09:53:25.402 | 1789570405402 | completion hook: `result=success exit_code=exited exit_status=0`, token `f4bd262d63dbe4bcc928b10c` |
| 09:53:25.449 | 1789570405449 | datagram received on the workstation (47 ms), sender 192.168.0.8 |
| 09:53:37.693 | 1789570417693 | read-out session 2234 opened; unit and timer inactive |

Boot id bound (launcher and driver on `882b9409-f9d4-4b4e-b2ca-bffc1de1f891`).
Journal collected once after the last child from the launch instant −1 s
to the last return +1 s: `collected`, 9 entries, 0 malformed. In the
separation interval: session 2233's close, deactivation, logout, and
removal (09:49:13.099–13.101), `sysstat-collect` starting and finishing
(09:50:03.9), one wireless `UFW BLOCK` (09:51:01.7). In the measurement
window: one wireless `UFW BLOCK` (09:53:06.7, in child 9's window) and
nothing else; the earlier `UFW BLOCK` at 09:51:01.7 precedes the window
by 14.7 s and belongs to the separation interval. **Separation: the logout precedes the first child by 123,313.8
ms. Window: zero session entries; `verified`.**

The recorded verdict and the corrected one. The driver at run time
reported `launch_separation.verdict = failed` with the reason "no
'Session 2233 logged out' entry on boot 882b9409-f9d4-4b4e-b2ca-bffc1de1f891
inside the collection": it required the entry's `_BOOT_ID`
(`882b9409f9d44b4eb2cabffc1de1f891`, as the journal spells it) to equal
the launcher's boot id as `/proc/sys/kernel/random/boot_id` spells it,
with dashes, and the strings differ. The precedence then withheld the
owner as `withheld_launch_separation_failed`, keeping the rule's outcome
beside it as `rule_outcome_withheld`, exactly as it would for a real
failure. The closing commit normalizes both spellings to their hex
digits before comparing, with a fixture that a dashed launcher id and
an undashed journal id on the same boot verify, and an `adr595-reevaluate`
verb that recomputes both verdicts and the precedence from the retained
summary's own collection and launch record and re-runs the evaluator
and the cutoff over the retained rows. Run on strix from the closing code
(`b83dfe90b314889214863c01b3786a581952c3029262d011a4133d49aac6a419`,
tree `e0039da1…`): separation `verified` (logout 1789570153101.0, first
launch 1789570276414.8, margin 123,313.8 ms), window `verified`, cutoff
`within_bound` at 0.0051, owner `resolved` on `inner_1_build_write`.
That first re-evaluation is retained as
`adr595-strix-host-record/reevaluation_round1_boot_id_normalized.json`
(`87919a2a…`). A second review round then tightened the evidence rules
themselves: the logout counts only as logind's own entry
(`systemd-logind.service`), with a boot id present and well formed (32
hex digits, dashed or not; anything else is malformed, and a malformed
launcher id makes separation `unknown`), and lying inside both the
separation interval and the collection's bounds, so a logout from an
earlier launch or from another source cannot verify this one; and the
re-evaluation derives the window from all eleven rows' launch and
return stamps, refusing a missing, non-finite, inverted, or overlapping
stamp rather than narrowing the window. Fixtures: wrong source, missing
boot id, malformed launcher id, a too-early logout, and the
window refusals. Re-evaluated again on strix from the closing code
(`1b781073b067454ac8758421ffe92191910eafe3a02dd1b062a0956833c9e8f8`,
tree `f7e4b99d…`): separation `verified` (the same logout, 1789570153101.0,
margin 123,313.8 ms, logind's entry on this boot inside the interval
and the bounds), window `verified` with no session entries, cutoff
`within_bound` at 0.0051, owner `resolved` on `inner_1_build_write`.
Report retained as `adr595-strix-host-record/reevaluation_under_tightened_checks.json`
(`7efd06ccd0fbd307d2ec5ed8d8a66be54f3b4880c561b35d441c54595c4aee63`);
the run's `summary.json` (`dd3a7a9c…`, with the recorded `failed`) and
`samples.jsonl` (`43d45f67…`) are unchanged. No launch or run was
repeated. The result read below is the re-evaluated one, and it is read
because the evidence for separation is in the retained collection in
full; the recorded verdict was the comparison's defect, not the
launch's.

## The run

Warmup (off) 8,987.6 ms. Pairs, off / on, movement: pair 0 (off, on)
9,005.8 / 9,158.9, +1.70%; pair 1 (on, off) 9,022.6 / 9,128.3, +1.17%;
pair 2 (off, on) 9,030.9 / 9,132.3, +1.12%; pair 3 (on, off) 8,992.7 /
9,178.2, +2.06%; pair 4 (off, on) 8,985.2 / 9,160.6, +1.95%. Off median
**9,005.8 ms**, off range **45.6 ms** (8,985.2 to 9,030.9), on median
9,158.9 ms. Perturbation gate: median `|m_i|` 0.0170, accepted. Every
profile complete, the six identities reconciled on every on row, facts
consistent on every pair. The off median sits 121.0 ms (−1.3%) from the
sealed q21 median and 58.8 ms (−0.7%) from ADR 593's, observed
distances.

**Variability: 0.0051 against the 0.04 cutoff. Within bound.**

| pair | `runtime_prep_inner_1` µs | sizing scan | partition sizing | build write | residual | branch | partitions | passes | sizing rows |
|---:|---:|---:|---:|---:|---:|---|---:|---:|---:|
| 0 | 1,403,764 | 201,804 | 271,884 | 930,060 | 16 | spilled | 1 | 1 | 5,845,938 |
| 1 | 1,381,418 | 201,144 | 266,253 | 914,005 | 16 | spilled | 1 | 1 | 5,845,938 |
| 2 | 1,353,663 | 200,353 | 267,071 | 886,225 | 14 | spilled | 1 | 1 | 5,845,938 |
| 3 | 1,348,843 | 201,023 | 267,620 | 880,187 | 13 | spilled | 1 | 1 | 5,845,938 |
| 4 | 1,352,782 | 201,044 | 267,584 | 884,140 | 14 | spilled | 1 | 1 | 5,845,938 |

Owner floor `max(500 ms, 2 × 45.6) = 500 ms`.

| term | median-scaled ms | share of off median | clears floor |
|---|---:|---:|:---:|
| `inner_1_build_write` | 876.4 | 9.73% | yes |
| `inner_1_partition_sizing` | 263.2 | 2.92% | no |
| `inner_1_sizing_scan` | 198.1 | 2.20% | no |
| `inner_1_residual` | 0.0 | 0.00% | no |

**Envelope owner: `inner_1_build_write`, resolved**, by ADR 594's rule:
one term clears the floor, it leads the next by 613.2 ms against a
45.6 ms range, and no residual leads. The term `runtime_prep_inner_1`
reads 1,338.6 median-scaled ms (14.86%) here, beside ADR 593's 1,345.0
and ADR 594's steady pairs; ADR 585's rule on the same rows returns it
resolved, as context. The build write is the spilled branch's create,
write of 5,845,938 rows to a `SpilledInnerJoinBuild`, and finish; the
sizing scan and the one partition-sizing pass, two full reads of the
filtered `orders` input before it, together cost 461 ms and neither
clears the floor on its own.

## The readings, in run order

| child | arm | execute ms | temp start→end °C | P-core clocks at start (MHz) | at end | ctxt vol/invol | reading µs | gap ms | journal |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| warmup | off | 8,987.6 | 83→82 | 800–5,900 | 800–5,994 | 11,732/57 | 393/275 | — | 0 |
| 1 | off p0 | 9,005.8 | 55→82 | 800–5,900 | 800–6,000 | 11,732/81 | 419/386 | 17 | 0 |
| 2 | on p0 | 9,158.9 | 88→73 | 800–5,900 | 800–5,777 | 11,733/76 | 378/242 | 16 | 0 |
| 3 | on p1 | 9,128.3 | 89→77 | 800–5,900 | 800–5,694 | 11,733/87 | 247/469 | 17 | 0 |
| 4 | off p1 | 9,022.6 | 90→73 | 800–5,900 | 800–5,408 | 11,733/75 | 473/308 | 21 | 0 |
| 5 | off p2 | 9,030.9 | 90→85 | 800–5,900 | 800–5,762 | 11,733/74 | 404/242 | 23 | 0 |
| 6 | on p2 | 9,132.3 | 88→80 | 800–5,829 | 800–5,877 | 11,733/79 | 384/290 | 8 | 0 |
| 7 | on p3 | 9,178.2 | 91→71 | 800–5,885 | 799–5,620 | 11,733/79 | 386/405 | 10 | 0 |
| 8 | off p3 | 8,992.7 | 88→83 | 800–5,823 | 800–5,778 | 11,730/74 | 235/278 | 6 | 0 |
| 9 | off p4 | 8,985.2 | 91→86 | 800–5,886 | 800–5,872 | 11,732/66 | 369/318 | 7 | 1 |
| 10 | on p4 | 9,160.6 | 91→66 | 800–5,833 | 800–5,999 | 11,734/102 | 247/276 | 4 | 0 |

Endpoint temperatures 83-91 °C at start readings (55 °C on child 1's,
an outlier at one instant) and 66-86 °C at end readings; one P-core
thread at 5.4-6.0 GHz at each endpoint; involuntary switches 57-102;
reading cost 235-473 µs; inter-child gaps 4-23 ms; journal entries in
children's windows: one wireless `UFW BLOCK` in child 9's. The limits ADR
593 stated apply. What this run does not establish: that ADR 594's
spread came from its session events; only that this window had none of
the specified activity and a range of 0.51%.

## Implementation and verification

Harness: `LifecycleDriverSpec.separated_launch`; `adr595_launch_from_environment`
reads the launcher's session id, the boot id, the attempt token, the
pre-scheduling launch instant, and the delay from the unit's
environment, binds the boot id (normalized), reads the timer's
`LastTriggerUSec` at start, and refuses a trigger more than 30 s after
the launch instant plus the delay or a driver start more than 30 s after
the trigger; the shared driver collects the journal from the launch
instant; `adr595_launch_verdicts` returns the separation verdict (the
launcher's logout on this boot, before the first child, `verified` only
under a `collected` collection covering the interval; `failed` for a
logout inside or after the window or absent; `unknown` for a logout
outside the collected bounds or any other collection state) and the
window verdict (`verified` with zero session entries under a covering
`collected` collection, `failed` naming them, `unknown` otherwise);
`adr595_apply_launch_precedence` withholds the owner as
`withheld_launch_separation_{failed,unknown}` or
`withheld_window_not_clean_{failed,unknown}` keeping the rule's outcome
beside it; `adr595-q21-inner-1` is ADR 594's driver under this spec; the
completion hook (`scripts/adr595_completion_hook.sh`, `ExecStopPost`)
writes the marker and sends the datagram; the workstation's wait
(`scripts/adr595_wait_completion.py`) accepts only this attempt's token
and ends with nothing at the deadline; `adr595-reevaluate` as above.
Tests: separation on every placement and collection state, the
dashed-versus-undashed boot id, another boot, a wrong source, a missing
or malformed boot id, a too-early logout, bounds starting after the
launch; the re-evaluation window requiring every stamp; the window with none and one event and under each state and
short bounds; precedence for each combination; late trigger, late
start, missing trigger, timestamp parsing; the wait ignoring a probe and
a foreign token, accepting the attempt's, and ending at the deadline
with exit 1 and nothing on stdout.

Ladder on bigfoot (`RUST_MIN_STACK=67108864`, fmt clean): Toron library
1,809, architecture 2,151, integration 3,649; toron-tpch library 241 (+4
ignored), `toron-tpch` binary 114, `toron-bench` 9, integration 261 (+2
ignored); the closing code's tpch suites and Toron library and
architecture re-run after each review round.

Evidence on strix: `target/tpch/adr595-q21-inner-1/` (`samples.jsonl`
`43d45f67ee72f2bbf4d73e4cfea63e3c1559f02c9f8382abb2399544e250e000`,
`summary.json` `dd3a7a9cf53fd8b41715c167953f2507a7b0950a6650a7fb94a2d80f26699aac`)
and `target/tpch/adr595-strix-host-record/` (`host_config_before.txt`
`3ff9b0dc…`, `host_after.txt` `c289a9c2…`, `completion.marker`
`418fe9ff…`, `driver_stdout.log` `d123bca8…`, `tool_versions_run_host.txt`
`d88e2dc9…`, `reevaluation_round1_boot_id_normalized.json` `87919a2a…`,
`reevaluation_under_tightened_checks.json` `7efd06cc…`).

## Closure

ADR 595 closes with the envelope's owner resolved on strix:
**`inner_1_build_write`**, the spilled branch's build write, 876.4 ms
(9.73% of q21), under a verified separated launch, a clean window, and a
0.51% off range; and with one instrument defect recorded and fixed (the
boot-id spelling), the retained summary's withheld verdict left as
written and the re-evaluated verdict read. The successor is the build
write's own decomposition before any cut: ADR 586's pattern applied to
`write_streaming_scan_inner_build_batches` and the spilled build's
finish (the scan read, the filter, the key hashing, the row write, the
spill, the finish). The separated launch is the standard strix procedure
from here.
