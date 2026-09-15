# ADR 593 - q21's envelope on strix: host readings on every child, then one decomposition: results

Measured 2026-09-15 on strix under ADR 591's coordinate as a transient
user service, through the predeclared start gate, with host readings on
every child and the journal collected once afterwards. **The run's off
range was 118.9 ms, 1.31% of its 9,064.7 ms median: inside the 4% bound,
so ADR 585's rule applies unchanged, and it returns `runtime_prep_inner_1`
as the envelope's owner, resolved: 1,345.0 median-scaled ms, 14.84% of
the off median, 491.5 ms ahead of `runtime_prep_semi_write` (853.5 ms)
against a 118.9 ms range and a 500 ms floor.** With ADR 592's phase 1
having resolved `outside_drive`, **q21's owner on strix is
`runtime_prep_inner_1`**, the partitioned `orders` build for replay 1.
slim's hypothesis is confirmed on strix's own evidence. The readings say
where the run stood while it ran and are reported as such; they do not
say why ADR 592's phase 2 spread.

## Identity

| identity | value |
|---|---|
| source tree (instrumented) | `c6a292a39f90…` (= `a235a367`, the plumbing with the gate before the driver's own work; the closing commit differs in the plan documents alone) |
| instrumented executable | `795fc17c4220b4778e259d992d795b0cb2c5e5c38a3b156ce9646a3604e5b187`, both arms of every pair |
| build manifest | `target/tpch/evidence/build/adr593-instrumented.manifest` (mini and strix; staged hash verified on strix) |
| recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580` |
| sealed baseline (bound by content, ADR 592's loader with its coordinate and identity checks) | `950ebc4b…` on tree `a962f3d8…`, artifact `d03935eb…`, q21 median 9,126.9 ms, affinity `0-15` |
| driver unit | `adr593-decomposition.service`, cgroup `/user.slice/user-1002.slice/user@1002.service/app.slice/adr593-decomposition.service`; Unix session id 387448 (`getsid(0)`), audit session id 2143 (`/proc/self/sessionid`) |
| unit settings | `CPUAffinity` empty, `CPUQuotaPerSecUSec` infinity, `CPUWeight` not set, `MemoryMax` infinity, `TasksMax` 135090, `Nice` 0, `OOMScoreAdjust` 200 |
| start gate | package 43,000 millic (bound 45,000), load 0.08 (bound 0.3) |

Every one of the eleven rows was admitted by ADR 592's validator
(affinity `0-15` / explicit / `0-15`, the sealed digest, `full_hash`,
the instrumented identity, the launch manifest and hash), and every row
carries both host readings with nothing missing. The off median sits
62.2 ms (−0.7%) from the sealed q21 median, an observed distance.

## Protocol deviations, recorded

The record allows one decomposition run and a launch outside any login
session's lifetime; getting there took two gate refusals, one interrupted
attempt that had executed an admitted warmup, a code change to the gate's
placement, a change to the launch method, and a polling habit that
churned login sessions through the window. Each is a deviation from the
plan as written and is recorded here as one.

The start gate refused twice before the run, both times correctly and
both times because of my own doing, and both refusals are retained
(`adr593-strix-host-record/gate_refusal_attempt{1,2}_stderr.log`, the
note beside them). Attempt 1 read 49 °C: the host snapshot script had
just run its own burst of shell commands on a P-core, and the gate was
read seconds later. Attempt 2 read 56 °C with the host at 40 °C twenty
seconds before launch: the gate had been placed after the driver's
full-hash verification of the 3.1 GiB dataset, so it measured the
driver's own burst. The gate was moved to the first thing the driver
does, before any work of its own (executable `813f6e0d…` became
`795fc17c…`; the earlier manifest is retained as
`adr593-instrumented-gate-after-verification.manifest`), and the launch
procedure now waits twenty-five seconds after the snapshot.

Attempt 3 passed the gate, executed and admitted its warmup (an off row
of 9,061.7 ms, in the retained journal), and was stopped eighteen
seconds in with no pairs, retained as
`target/tpch/adr593-q21-decomposition-stopped-user-manager-teardown/`.
It is an interrupted attempt under the plan's one-run rule, and the run
below is a restart after a launch correction, not a second measurement
of a completed first; the rule is read that way here and the reading is
stated rather than assumed.
The service was outside the login session's scope as the record
requires, but the user manager `user@1002.service` itself shut down when
the launching SSH session logged out (`Linger=no` for the user), taking
every transient unit with it; the journal shows "Stopping
adr593-decomposition.service" at 16:55:19. `loginctl enable-linger me`
(accepted for the user's own account, `Linger=yes` since) makes the user
manager persist across sessions, and the ADR's launch method is amended
to require it. The SSH polling that churned sessions through the window
is the fourth deviation and is described with the journal below.

## The run

Started 16:56:35 CDT at 43 °C and load 0.08 (the gate's readings),
finished 16:58:47, host after at 46 °C and load 0.83 (read four seconds
later), spill scratch empty. Warmup (off) 8,968.1 ms. Pairs, off / on,
movement: pair 0 (off, on) 8,962.2 / 9,085.8, +1.38%; pair 1 (on, off)
9,024.2 / 9,127.1, +1.14%; pair 2 (off, on) 9,066.0 / 9,146.4, +0.89%;
pair 3 (on, off) 9,064.7 / 9,134.2, +0.77%; pair 4 (off, on) 9,081.1 /
9,209.2, +1.41%. Off median **9,064.7 ms**, off range **118.9 ms**, on
median 9,134.2 ms. Perturbation gate: median `|m_i|` 0.0114, accepted.
Every profile complete, the three bands reconciled.

**Variability: `range_off / median_off = 0.0131`, bound 0.04, within
bound.** ADR 585's rule applies. Owner floor `max(500 ms, 2 × 118.9) =
500 ms`.

| term | median-scaled ms | share of off median | clears floor |
|---|---:|---:|:---:|
| `runtime_prep_inner_1` | 1,345.0 | 14.84% | yes |
| `runtime_prep_semi_write` | 853.5 | 9.42% | yes |
| `runtime_prep_anti_write` | 704.8 | 7.78% | yes |
| `cleanup` | 427.6 | 4.72% | no |
| `runtime_prep_anti_read` | 390.8 | 4.31% | no |
| `runtime_prep_semi_read` | 280.0 | 3.09% | no |
| `runtime_prep_semi_finish` | 248.5 | 2.74% | no |
| `runtime_prep_anti_finish` | 229.3 | 2.53% | no |

(the remaining seven terms are below 20 ms each; `runtime_prep_residual`
leads them and no residual leads the table). Three terms clear the
floor; the leader's margin over the second, 491.5 ms, exceeds the
118.9 ms range, so the rule resolves: **`runtime_prep_inner_1`**. The
shared-source removable read is 280.0 ms, closed below floor as before.

Beside ADR 592's phase 2 (same coordinate, same instrument, one plumbing
commit older) the terms are within 1%: `runtime_prep_inner_1` 1,334.5 →
1,345.0, `runtime_prep_semi_write` 849.6 → 853.5, `runtime_prep_anti_write`
701.7 → 704.8, `cleanup` 424.7 → 427.6. The difference between the two
runs is the range: 609.0 ms there, 118.9 ms here. Beside slim's ADR 589
reading (1,671.4 / 952.8 / 790.3 / 561.0), host-to-host.

## The readings, in run order

Per child: execute time; package temperature at the start reading and at
the end reading; the P-cores' clock range across the sixteen threads at
each endpoint; the reading thread's context-switch delta (voluntary /
involuntary) over the interval between its two counter reads; the two
readings' own durations; the gap since the previous child returned; and
the journal entries joined to the child's window.

| child | arm | execute ms | temp start→end °C | P-core clocks at start (MHz) | at end | ctxt vol/invol | reading µs | gap ms | journal |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| warmup | off | 8,968.1 | 86→80 | 800–5,900 | 800–5,591 | 11,733/83 | 374/318 | — | 12 |
| 1 | off p0 | 8,962.2 | 89→82 | 800–5,900 | 800–5,900 | 11,733/76 | 395/276 | 6 | 8 |
| 2 | on p0 | 9,085.8 | 90→77 | 800–5,900 | 800–5,827 | 11,733/85 | 409/269 | 6 | 8 |
| 3 | on p1 | 9,127.1 | 90→80 | 800–5,837 | 800–5,900 | 11,733/82 | 406/247 | 7 | 8 |
| 4 | off p1 | 9,024.2 | 90→78 | 800–5,805 | 800–5,852 | 11,733/82 | 263/441 | 6 | 8 |
| 5 | off p2 | 9,066.0 | 90→85 | 800–5,813 | 800–5,900 | 11,733/91 | 402/281 | 5 | 8 |
| 6 | on p2 | 9,146.4 | 91→78 | 800–5,889 | 800–5,717 | 11,732/82 | 379/287 | 7 | 11 |
| 7 | on p3 | 9,134.2 | 92→67 | 800–5,869 | 800–5,916 | 11,734/65 | 367/289 | 5 | 16 |
| 8 | off p3 | 9,064.7 | 91→80 | 800–5,900 | 800–6,000 | 11,733/90 | 387/251 | 5 | 17 |
| 9 | off p4 | 9,081.1 | 92→80 | 800–5,900 | 800–5,587 | 11,733/86 | 387/505 | 6 | 8 |
| 10 | on p4 | 9,209.2 | 91→81 | 800–5,826 | 800–5,603 | 11,733/76 | 393/233 | 4 | 8 |

What the readings show, and their limits. The package reads 86-92 °C at
the start reading of every child and 67-85 °C at the end reading; the
gate had read 43 °C before the driver began, and what raised the
package between the gate and the first start reading, and what lowered
it between each child's two readings, is not established by two
instants per child. At each endpoint one P-core thread reads 5.6-6.0 GHz
and the other fifteen 800 MHz (cpu 10 at the start reading of every
child; at the end reading cpu 10 and, on some children, a second
thread); the endpoint clocks show no clock limit at the instants read
and say nothing about the seconds between. The reading thread's
involuntary context switches over its interval are 65-91 on every child
and its voluntary count 11,732-11,734; what those switches are is not
established, and the count is one thread's over one interval, excluding
other workers. Each reading cost 233-505 µs, outside the execute clock;
the readings' presence before the clock may itself have touched caches
and power state, identically on both arms. Inter-child gaps are 4-7 ms,
and no journal query sits in them.

The journal: collected once after the last child and a two-second
settle, `collected`, 101 entries in the run's window, none truncated, no
malformed records. Joined per child with every window inside the
collection's bounds and coverage `complete`: 8-17 entries in every
window, all of them `sshd`, `systemd-logind`, and `init.scope` session
churn (twelve login sessions started in the window) with one
`session-2148.scope` close and one kernel `UFW BLOCK` on the wireless
interface. The session churn is a protocol deviation of my own: the
launcher on slim polled the service's state over a fresh SSH connection
every ten seconds, and each poll opened and closed a login session on
strix during the window. It is a background activity this record
introduced, present on every child alike; it is reported, not netted,
and the next run polls without creating sessions. No `anacron`,
`sysstat`, or `fwupd` event fell in the window.

The retained slow samples of ADR 592's phase 2 have no readings, so
nothing here explains them; what this run shows is that under the same
coordinate, with the driver outside any login session's lifetime and the
gate satisfied, the cell repeated inside the bound. Whether ADR 592's
range came from the session teardown inside its window, from the one
minute of settle after a 68 °C finish, or from something else is not
established, and this record does not claim it.

## The selection

ADR 592's phase 1 named `outside_drive`; this run resolves the envelope
to `runtime_prep_inner_1`. **q21's owner on strix is
`runtime_prep_inner_1`**, 1,345.0 ms, 14.84% of the off median, by ADR
585's rule on a run inside the predeclared bound. No optimization opens
here; the successor decomposes `runtime_prep_inner_1`'s lifecycle, which
has none yet.

## Implementation and verification

`toron_tpch::host_readings`: `take_reading` over given roots (coretemp by
hwmon name and `Package id 0` label at any index; `scaling_cur_freq` for
every CPU in the affinity mask, the whole source failing if any one CPU
cannot be read, with the CPU named; the reading thread's counters from
`/proc/thread-self/status` with its PID and TID; first and last
timestamps and `elapsed_us`), each source `null` with its reason when
unreadable; `HostReadingsPair::ctxt_switch_delta` suppressed across
threads or decreasing counters, with the reason; `begin`/`finish` armed
only by `perf-run-one` and bracketing the execute clock in
`execute_logical_with_catalog_timed`, the pair handed to the row as
`host_readings` (this process runs exactly one query, and the slot is
emptied when read); `collect_journal` once per run under the 30 s and
2,000-entry bounds, keeping realtime and monotonic timestamps and boot
id, with `collected` / `truncated` (which bound) / `unavailable` (reason)
distinguished, and `join_window`; `observe_driver_scope` refusing a
`session-*.scope` cgroup and recording cgroup, `getsid(0)`, the audit
session id, and the unit's settings. The ADR 585 driver: scope and gate
first, before any work of its own; launch and return stamps on every
row; the journal collected after a two-second settle and joined per
child with gaps; the variability bound applied to the evaluated summary
(`closed_on_variability` above 0.04, the leading term kept as such and
no owner named). Tests: a fabricated sysfs/procfs tree yields every
field and only the masked CPUs; each missing source is null with its
reason and the others intact; coretemp found at any index; the delta
suppressed across threads and on a decrease; the readings bracket a
fabricated span with their cost outside it; journal lines parse with
timestamps, boot id, byte messages, and the cap; an unavailable journal
is named; the login-session cgroup is refused and a service named; the
bound on both sides and at the boundary; the journal joined per child
with gaps from the stamps and an uncovered window said so.

## Review round: the collector bounded while reading, and coverage by state

Two gaps were closed under review after the run, in the closing commit.
The journal collector used to read all of the command's output into
memory before applying the entry cap, with stderr undrained while the
command ran, and it reported `collected` whatever it had skipped. It now
runs the command in its own process group, reads line by line applying
the entry cap and a 16 MiB byte cap as lines arrive, drains stderr on
its own thread, enforces the duration bound while reading, terminates
and reaps the whole group when a bound is hit or a read fails, counts
malformed records, and reports four states: `collected` (ran to
completion inside every bound, every record parsed), `truncated` (which
bound, and that the kept entries are a prefix), `degraded` (a read
failure, malformed records, or a failure after output), and
`unavailable` (nothing usable). Tests drive it with scripts: an
oversized flood cut at the entry cap and at the byte cap with the
command terminated, twenty thousand stderr lines not stalling a clean
collection, a hanging command terminated at a 300 ms bound in well under
a second, two malformed records counted and the state degraded, a
failing command unavailable with its stderr, a failure after output
degraded, a missing program unavailable. And the per-child join now
separates "inside the collection's requested bounds" from coverage: a
window is `complete` only under a `collected` collection, `partial` with
a count stated as a lower bound under a truncated one, and `unknown`
with no count under a degraded or unavailable one, whatever the bounds
say; fixtures for each.

A third round tightened the collector again. The byte cap had been
applied after a whole line was buffered, so a large newline-free record
bypassed the memory bound and whitespace-only lines escaped the check,
and stderr was read without bound before its prefix was kept; the
deadline stopped being enforced once the direct child exited, so a
descendant holding the pipe (`sleep 30 & exit 0`) defeated the bound,
while stdout reaching EOF terminated a command that was still
legitimately finishing. Now the pipe is read in 64 KiB chunks with the
byte cap checked on bytes read before any are buffered (whitespace counts
like anything else), stderr is drained with only a 2,000-byte prefix
retained, both readers observe the deadline through `poll` so no
descendant can hold the collection past it, the command's exit and each
reader's completion are tracked separately with the deadline enforced
until all three are done, and the reader signals why it stopped: EOF
lets a still-running command finish inside the bound, a cap or a read
failure terminates the process group at once. Tests: an oversized
newline-free record cut at the byte cap with the tally never above it, a
whitespace flood cut at the byte cap, a 10 MB stderr flood beside a clean
collection and a failing variant whose reason carries only the bounded
prefix, the orphaned `sleep` bound held at 300 ms after the command's
exit, and a command that closes stdout, works on, and exits 0 collected
clean.

A fourth round closed one more classification path, found by the
reviewer on bigfoot with `sleep 1 >/dev/null & exit 0` under a 100 ms
deadline: a descendant holding only stderr let the command exit and
stdout reach EOF, the stderr drain stopped at its deadline but reported
only its text, and the coordinator saw an exited child and finished
readers and called the collection `collected`, ten times out of ten. The
drain now returns why it stopped beside its prefix; a drain that hit the
deadline or failed terminates whatever remains of the group and the
collection is `truncated` at the duration bound (naming stderr as the
pipe still open) or `degraded` for a drain failure, never `collected`.
The fixture runs the stderr-only orphan five times under a 100 ms bound
and requires zero clean verdicts.

The retained collection was rechecked under each collector in turn on
strix by an `adr593-journal-recheck` verb, which re-collects the retained
summary's window and re-joins it to the retained rows; under the closing
collector (`9f180316297a010d4e0397fc419bb18ed6e429b185da69c4fb84234b1596431f`,
tree `32470cea…`, recipe `9af51cbc…`, manifest
`evidence/build/adr593-recheck.manifest`, staged under a neutral root
because the build host's checkout lives at another path): state
`collected`, 101 entries, 0 malformed records, 115,106 bytes read, the
entry set identical to the retained one (nothing only in either), and
every child's coverage `complete` with the counts above. Report retained
as `adr593-strix-host-record/journal_recheck_under_corrected_collector.json`
(`766bcf7c44eeb8017039b1e809155a9ea4b9f03a7e574876e0cd5d237978d1a1`; the
earlier rounds' reports are kept as `journal_recheck_round2.json`
(`d5d75126…`) and `journal_recheck_round3.json` (`0ec24076…`)); the run's
`summary.json` and `samples.jsonl` are unchanged. No performance run was
repeated.

Validation hosts. mini, the build host for the measurement and the first
two rounds, dropped off the network on 2026-09-15 at about 20:50 CDT
while running the third round's ladder, after toron library 1,806 and
toron-tpch library 240, binaries 105 and 9, and integration 261 had
passed there; it was found hot to the touch the next morning and its
previous boot's journal ends without a shutdown record or any kernel
thermal or machine-check line, which does not establish the cause. That
is an infrastructure finding on its own, apart from the ADR 538
atlas-driver fault seen only there. The third round's validation was
finished on bigfoot (i9-12900, the same recipe `9af51cbc…` reproduced
under rustc 1.95.0, clang 21.1.8, mold 2.40.4, a separate checkout at
`~/work/ket-toron`): fmt clean; focused `host_readings` 9 and driver
`adr591`/`adr592`/`adr593` 10; then Toron library 1,806, architecture
2,151, integration 3,649; toron-tpch library **241** with the ADR 538
atlas-driver test included and passing (94 s), binaries 105 and 9,
integration 261 (+2 ignored); after the fourth round, fmt clean, the
focused 9 and 10 again, toron-tpch library 241, Toron library 1,806 and
architecture 2,151. The closing executable above was built there.

Evidence on strix: `target/tpch/adr593-q21-decomposition/`
(`samples.jsonl` `a0a521dbc657ce25347a4936835608bf639ae6b10104a1a7cfb6211e5aff807e`,
`summary.json` `c284a7601c5e51549b29ef8cffe5724d765ce9a333f4ed56898c4648f61333de`),
the stopped attempt's directory (`samples.jsonl` `26f49e0c…`), and
`target/tpch/adr593-strix-host-record/` (`host_config_before.txt`
`f323e2c8…`, `host_after.txt` `05594a91…`, `driver_stdout.log`
`4eab65ae…`, the two gate refusals `278a5b86…` and `f9856925…`, the
note `1dff07d1…`, the attempt snapshots `2b496e0f…`, `9658acf3…`,
`def6afb8…`, `d32b2843…`, and `tool_versions_run_host.txt` `d88e2dc9…`).

## Closure

ADR 593 closes with q21's owner on strix resolved to
`runtime_prep_inner_1` under the predeclared bound, the readings
reported with their limits, and four recorded protocol deviations (two
gate refusals, one interrupted attempt with an admitted warmup, and the
session churn from polling) that changed the gate's placement, the
launch method, and the polling procedure; the collector was tightened
three times under review and the retained collection rechecked identical
each time. Successor: the lifecycle decomposition of
`runtime_prep_inner_1`, the partitioned `orders` build for replay 1,
before any cut. Nothing else under this number.
