# ADR 593 - q21's envelope on strix: host readings on every child, then one decomposition

**Status:** PROPOSED. Successor to ADR 592, which left q21's owner on strix
unresolved between `runtime_prep_inner_1` and `runtime_prep_semi_write`
under a 609.0 ms off range.


## Context

ADR 592's phase 2 ran ADR 585's decomposition on strix under ADR 591's
coordinate: `runtime_prep_inner_1` led at 1,334.5 median-scaled ms and
cleared the floor, `runtime_prep_semi_write` followed at 849.6 ms, and
the 484.9 ms margin sat inside the five off samples' range of 609.0 ms,
so the rule reported the tie unresolved. Phase 1, two minutes earlier
under the same coordinate, had held its five off samples inside 35.7 ms.
Phase 2's warmup and its last two pairs were the slow samples (9,368.0;
then 9,504.4 on and 9,564.8 off, 9,451.0 on) against 8,955.8-9,103.0 for
the rest.

What the retained evidence says about that range, and what it cannot
say. The run began one minute after phase 1 ended with the package at
68 °C, on one continuously loaded P-core, and ended at 46 °C; no
temperature or frequency was read during either run. The system journal
for phase 2's window, read afterwards, shows `anacron.service` starting
at 14:34:29 (no jobs), the SSH session scope that had launched the run
logging out at 14:34:29 with logind "waiting for processes to exit"
(the driver had been started inside that session's scope), and the
`rtw88_8821au` USB wireless driver logging a firmware failure at
14:32:08; phase 1's window shows a `CRON` anacron invocation at 14:30:01.
The rows carry no timestamps, so none of these can be placed against a
sample. The host's timers include `sysstat-collect` every ten minutes and
`fwupd-refresh`. The affinity pin excludes the E-cores but says nothing
about migration among the sixteen P-core threads. Each of these is a
hypothesis; none is supported or excluded by evidence taken during the
run, and ADR 592 said so and declined to attribute.

Repeatability readings of q21's Toron cell on this coordinate so far,
as `(max − min) / median` over five samples: 0.004 (ADR 592 phase 1,
pinned), 0.022 (ADR 591's sealed board, pinned), 0.004, 0.034, 0.022
(ADR 590's three unpinned boards), and 0.067 (ADR 592 phase 2, pinned).
One of six readings is far from the others; that is the fact the bound
below is set against.

A new record number does not license running the decomposition again
until an owner appears. The finding to act on first is that the drivers
cannot tell a variability event from a repeatable measurement, because
the rows record nothing about the host at the time of the sample.


## Decision

**Every child records host readings, identically on both arms, outside
the timed span.** The `perf-run-one` child takes one reading immediately
before its execute clock starts and one immediately after it stops, and
puts both on its row as `host_readings.start` and `host_readings.end`.
Each reading is:

- `unix_ms` (`CLOCK_REALTIME`) and `monotonic_ns` (`CLOCK_MONOTONIC`),
  so the sample can be placed against the journal and against the other
  reading;
- `package_temp_millic` from the `coretemp` hwmon device's `Package id 0`
  input, located by hwmon name and label, not by index (`hwmon5` today,
  not promised);
- `cpu_freq_khz`, a map from each CPU in the process's affinity mask to
  `scaling_cur_freq`, so the sixteen P-core threads' clocks are read and
  the E-cores are not;
- `pid` and `tid` of the reading thread, and that thread's
  `voluntary_ctxt_switches` and `nonvoluntary_ctxt_switches` from
  `/proc/thread-self/status` (the thread's own counters; `/proc/self/status`
  is the thread-group leader's and is not used). The summary reports the
  difference between the two readings only when both carry the same PID
  and TID and neither counter decreased, and labels it as the count over
  the interval between the two counter reads, which contains the execute
  span and the other readings taken beside it, for that one thread; the
  runtime's other workers are not covered.
- `elapsed_us`, the wall time from the reading's first read to its last.

Every source is optional and independent: a source that cannot be read
is recorded as `null` with its reason under `host_readings_missing`,
never as zero and never as a refusal, since the readings are diagnostic
evidence and the row's validity does not rest on them. A reading is not
an instant: it is some twenty sequential sysfs and procfs reads, and its
two timestamps are taken first and last so its own interval is on the
row. There is no sampling inside the execute span, and the readings' own
cost sits in the row's `total_ms` and `parent_wall_ms` where every other
non-execute cost already lives; expected cost is under two milliseconds
per reading, and `elapsed_us` measures it on every row rather than
asserting it. Outside the execute clock does not mean the execution is
unperturbed: reads taken just before the clock starts can touch
scheduling, caches, and power state, and the record claims only that the
cost is measured and identical on both arms. The endpoint frequencies
are two instants' clocks on sixteen threads, not an average over the
execution and not evidence of migration; the context-switch delta is
one thread's count over one interval. These limits are stated with the
readings wherever they are reported.

**The driver collects the journal once, after every child has finished,
and joins it to the recorded windows.** Querying after each child cannot
reliably collect events through `end + 1 s`, which may still be in the
future, and it would put the query's duration into the inter-child gap.
So the ADR 585 driver stamps `driver_launch_unix_ms` and
`driver_return_unix_ms` on every row as it runs, and after the last
child returns it waits at least two seconds, then reads the system
journal once for `[first launch − 1 s, last return + 1 s]` with
`journalctl --system -o json --since --until`, under a bounded duration
(30 s) and a bounded output (entries capped at 2,000 and the cap
stated). Each entry keeps its realtime timestamp, its monotonic timestamp
and boot id where the journal provides them, its unit or syslog
identifier, and its message's first 200 characters. The summary joins the
entries to each child's `[launch − 1 s, return + 1 s]` window and reports
per child the count and the entries; a child's window can be joined
only from a collection that covers it, so the collection's own bounds
are recorded beside the joins. Three states are distinguished and
recorded: `collected` (the query returned within its bounds),
`truncated` (the entry cap or the duration bound was hit, with which and
how far), and `unavailable` (the command failed or the journal was not
readable, with the reason). Inter-child gaps are reported from the launch
and return stamps and contain no journal query.

**The driver runs as a transient user service, outside any login
session's scope.** ADR 592's phase 2 ran inside the SSH session scope
that launched it, and that scope's teardown fell inside the window.
`setsid` would not have helped: it creates a Unix session, not a systemd
scope, and leaves the process in the login cgroup. The driver is started
with `systemd-run --user --unit adr593-decomposition …`, a transient
service under `user@<uid>.service`, managed independently of the SSH
session; before the warmup the driver reads its own cgroup from
`/proc/self/cgroup` and refuses to run if the path contains a
`session-*.scope`, and it records the cgroup path, the Unix session id from
`getsid(0)` and, under its own label, the audit session id from
`/proc/self/sessionid` (three separate observations), and the unit's
resource settings as `systemctl --user show` reports them (`CPUQuota`,
`CPUWeight`, `MemoryMax`, `TasksMax`, `Nice`, `CPUAffinity`, `OOMScoreAdjust`)
in the summary; the results quote them. The affinity coordinate is still
the harness's own `TORON_TPCH_CPU_AFFINITY`, applied and recorded as ADR
591 fixed, and the unit sets no affinity of its own.

**The variability bound is predeclared: `range_off ≤ 0.04 × median_off`.**
It is an operational cutoff fixed before the run, not a statistical
confidence bound, and its rationale is the historical spreads alone:
five of the six five-sample readings of this cell on this host lie at or
below 0.034, pinned and unpinned alike, and the one that does not (0.067)
is the run this record exists to understand; four percent lies above
every ordinary reading and below the exceptional one. If the run's off
range exceeds the bound, the record closes on the variability finding
with the readings reported against the slow samples, and no owner is
read from it. If it does not, ADR 585's owner rule is applied to the run
exactly as ADR 592 applied it (fifteen terms, `max(500 ms, 2 ×
range_off)`, ties inside `range_off` unresolved,
residual-leads-is-an-instrument-finding), and its verdict is the
envelope's owner or its tie; a run inside the bound may still return a
tie, and that is the rule's verdict, not this bound's.

**A start gate, predeclared.** The run starts only with package
temperature at or below 45 °C, load below 0.3, the spill scratch empty,
the output directory absent, and ADR 590's freeze re-verified; the gate
readings are the snapshot's. A temperature that cannot be read does not
satisfy the gate: the gate needs the reading, and the run waits until it
has one, whereas a missing reading on a child's row afterwards is
diagnostic and non-refusing. This is a start condition, not a control:
it makes the run comparable to phase 1's start (42 °C, load 0.00), and it
is recorded, not tuned.

**One decomposition run, and only that.** ADR 585's driver, one warmup
and five pairs, bound to ADR 591's sealed manifest as ADR 592's plumbing
requires, on one instrumented release executable built on mini from this
tree. No second run under this number whatever the outcome; a refused or
rejected run is retained and reported. ADR 584's phase is not repeated:
its `outside_drive` result stands as ADR 592's, and this record decides
only the envelope.

**Readings are evidence, not attribution.** The results report the
readings beside every sample, slow or not, and say what they show: a
temperature or clock that differs on the slow samples, a journal event
inside a slow sample's window, a context-switch count that differs. A
cause is named only where the readings support it; otherwise the
readings are reported and the cause is left open. The record does not
change the CPU policy, the timers, the wireless device, or anything else
about the host; anything the readings suggest changing is a coordinate
change for its own record.

**No optimization**, no engine change, no change to ADR 585's instrument
or rule.


## Acceptance

- The reading module reads each source from a directory root it is given,
  so tests run it over a fabricated sysfs/procfs tree: a complete tree
  yields every field; a tree missing the hwmon device, a CPU's cpufreq
  file, or the status file yields `null` for that field with its reason
  and every other field intact; the coretemp device is found by name and
  label at any hwmon index; only CPUs in the affinity mask are read.
- A `perf-run-one` row carries `host_readings.start` and `.end` with
  `end.monotonic_ns ≥ start.monotonic_ns` and the execute span between
  them; the readings are taken outside the execute clock (tested by
  bracketing a fabricated execute with readings whose `elapsed_us` is
  excluded from it); each reading carries the reading thread's PID and
  TID and its own two timestamps; both arms and the warmup carry them
  identically.
- The context-switch delta is reported only when both readings carry the
  same PID and TID and neither counter decreased; fixtures cover a TID
  change and a decreasing counter, each suppressing the delta with the
  reason stated.
- The journal collection is a function of a window returning parsed
  entries with realtime and, where present, monotonic timestamps and
  boot id; over canned `journalctl -o json` lines it yields the entries,
  joins them to per-child windows, and reports `collected`; hitting the
  entry cap or the duration bound reports `truncated` with which bound;
  a failing command reports `unavailable` with the reason; in every case
  the rows remain admitted.
- The driver stamps launch and return times on every row, the summary
  reports each inter-child gap, and no journal query runs between
  children.
- The driver refuses to start inside a `session-*.scope` cgroup and
  records its cgroup path, Unix session id (`getsid(0)`), audit session id
  (`/proc/self/sessionid`, labelled as such), and the unit's resource
  settings; a fixture over a fabricated cgroup file covers the refusal
  and the acceptance.
- The summary carries `variability: { range_off_ms, median_off_ms, bound,
  ratio, verdict }` with the bound fixed at 0.04; when exceeded,
  `owner.status = closed_on_variability` and no term is named as owner;
  a fixture on each side of the bound.
- The results document quotes the start gate readings, the driver's
  cgroup, Unix and audit session ids, and unit settings, every sample's readings
  (temperature, the P-cores' clock range at each endpoint, the thread's
  context-switch delta where reportable, the joined journal count) in run
  order with the readings' limits stated, the inter-child gaps, the
  journal collection's state and bounds, the variability verdict, and
  then either the fifteen-term table with ADR 585's verdict or the
  readings against the slow samples with the variability finding.
- Plan commit, then one implementation/results/closure commit; the
  measured tree named; nothing else under this number.


## Guardrails

- Collection is identical on the off and on arms and on the warmup; the
  execute span contains no reading.
- The bound and the start gate are fixed here and not revisited after the
  run; a run that fails the start gate waits for the gate, it does not
  change it.
- Build on mini, measure on strix, nothing else on strix in the window,
  the host record taken and hashed, as ADRs 590-592 prescribe.
- A missing reading never refuses a row and never becomes a zero.


## Risks

- Endpoint readings may show nothing distinctive on a slow sample, and
  they cannot show what happened between the endpoints; then the record
  closes with the readings reported and the cause still open, and the
  next record asks a different question (continuous sampling from a
  sibling process outside the affinity set, or the wireless device as a
  coordinate item), not the same one again.
- `journalctl` access depends on group membership (`adm` on this host);
  the row records unavailability rather than assuming an empty window.
- A run inside the bound may still return a tie; that is ADR 585's rule
  speaking, and the record reports it as such.


## Forward

- If the rule returns `runtime_prep_inner_1`, that term has no lifecycle
  decomposition: the next record decomposes it before any cut. If it
  returns `runtime_prep_semi_write`, ADR 586's decomposition applies on
  strix. If the bound closes the record, the next record is about the
  host, not about q21.
- The readings belong on every driver row in the arc, not only this
  one's; once they have shown their worth here, the ADR 584 and paired
  drivers adopt them under a plumbing record.
