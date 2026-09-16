# ADR 595 - q21's replay-1 preparation on strix, again: the same decomposition under a verifiably separated launch

**Status:** PROPOSED. Successor to ADR 594, which closed on the variability
cutoff with the band reconciled and the branch facts established.


## Context

ADR 594 ran its one decomposition and closed on a 799.1 ms off range
(8.90% of the median) against the 4% cutoff. The band worked: the facts
established that q21's replay-1 preparation at SF=8 under 2 GiB takes the
spilled branch (one partition, one sizing pass, 5,845,938 rows) and that
on the two steady pairs the 1.34 s term is about 880 ms of build write,
266 ms of partition sizing, and 200 ms of sizing scan. No owner was read.

The window of that run contained login-session events of my own: the SSH
session that launched the unit with `--wait` was terminated by the
launching workstation 53 seconds in (logind tore down `session-2205`
inside child 4's window), and a check session and a replacement wait
session followed inside children 5 and 6. The slow samples were children
6 through 10 on both arms. ADR 593's run had carried the same kind of
churn from polling. Neither run established that the session events
caused the spread, and this record does not claim it either: what the
launch procedure below does is remove a known source of activity from
the window and verify its absence. Verified separation establishes the
absence of the specified session activity; it establishes nothing about
the cause of any variability that remains.

Everything measured stays as ADR 594 fixed it: the sixth band, ADR 591's
coordinate, ADR 592's binding and validator, ADR 593's readings,
journal, scope record, start gate, and the 4% cutoff, ADR 594's four-term
rule with its named outcomes and the leading-residual-is-incomplete
contract, the executable built on bigfoot from this tree, and one run.


## Decision

**The launcher's session ends before the unit starts, and the record
proves it from a collection that reaches back to the launch.** The
launch session on strix takes the host snapshot, then schedules the
driver as a transient user service with a delayed start and exits. It
records, into the host record and into the unit's environment, the
launch's identity and instants: its own login session id
(`ADR595_LAUNCHER_SESSION_ID`, from `loginctl` on `$XDG_SESSION_ID`),
the host's boot id (`ADR595_BOOT_ID`, from
`/proc/sys/kernel/random/boot_id`), a random attempt token
(`ADR595_ATTEMPT_TOKEN`, generated for this launch alone), and the
pre-scheduling instant (`ADR595_LAUNCH_UNIX_MS`, taken before
`systemd-run` is invoked, so it is in the environment and precedes
everything the launch does). After `systemd-run` returns, the launch
session records the timer's actual scheduled activation
(`NextElapseUSecRealtime` of `adr595-inner-1.timer`) into the host
record, beside the delay; the driver reads the timer's actual trigger
(`LastTriggerUSec`) at its own start for the late-start check, so the
coverage instant and the activation instant are two records, not one.
The driver stamps them all on the summary, refuses to run if its own
boot id differs from the launcher's, and collects the journal not from
the first child's launch but from `ADR595_LAUNCH_UNIX_MS − 1 s` through
the last child's return `+ 1 s`, so the launcher's logout, which falls
in the delay before the first child, is inside the collection. Two intervals are then checked
separately, each on its own coverage: the **separation interval**
`[scheduled − 1 s, first child's launch)`, in which the logind "Session
N logged out" entry for the launcher's id, on the same boot id, must
appear and its timestamp must precede the first child's
`driver_launch_unix_ms` (both quoted); and the **measurement window**
`[first launch − 1 s, last return + 1 s]`, in which no login-session
entry — `sshd`, `systemd-logind`, a `session-*.scope`, or `init.scope`
session start or stop — may appear. Each interval's verdict is `verified`,
`failed` (the logout inside or after the window; a session event in the
window, named with its child), or `unknown`.

**Missing journal evidence is unknown, never clean.** A verdict of
`verified` on either interval requires the driver's collection to be
`collected` (the command ran to completion inside every bound, every
record parsed) and its bounds to cover the whole interval; a `truncated`,
`degraded`, or `unavailable` collection, or bounds that do not reach the
interval, make that interval `unknown`, and a launcher logout that lies
outside the collected bounds makes separation `unknown`, not verified.
Fixtures cover each collection state and a logout outside the bounds.

**Precedence.** A separation or cleanliness verdict of `failed` or
`unknown` closes the record without promoting an owner: the owner is
withheld as `withheld_launch_separation_failed` / `_unknown` or
`withheld_window_not_clean` / `_unknown`, and the four-term table, the
facts, the readings, and the cutoff verdict remain as diagnostic
evidence. The cutoff is applied and reported in every case, and the rule
is applied only when both intervals are `verified` and the cutoff is not
exceeded. The record does not attribute any outcome to a session event
it finds; it names the event and stops.

**Termination is bounded by the unit's own settings, and completion is
signalled to the workstation without a session on strix.** The
transient service is created with `systemd-run --user --unit
adr595-inner-1 --on-active=120 --timer-property=AccuracySec=1s
-p Type=exec -p RuntimeMaxSec=600 -p TimeoutStopSec=30
-p KillMode=control-group -p KillSignal=SIGTERM -p SendSIGKILL=yes
-p FinalKillSignal=SIGKILL -p ExecStopPost=<completion hook>`;
lingering is enabled so the user manager outlives the launch session.
`RuntimeMaxSec` initiates termination at ten minutes (against about
2.5 minutes expected); the stop timeout then bounds how long the service
may take to stop before the whole control group is killed. The driver
refuses to run (a retained refusal) if its start is more than 30 seconds
after the timer's actual trigger, or the trigger more than 30 seconds
after the launch instant plus the delay, so a late activation is a
finding and not a longer window. The completion hook runs in
`ExecStopPost` after the service has stopped, whatever the result; it
writes a completion file beside the output directory carrying the
attempt token, `$SERVICE_RESULT`, `$EXIT_CODE`, and `$EXIT_STATUS`, and
then sends the same record as one UDP datagram to the workstation's
listener (`slim:7745`), a message and not a login: no session opens on
strix. The workstation's read-out procedure listens for a datagram
carrying this attempt's token and opens its one read-out session only
on receiving it; a datagram with another token, or none, authorizes
nothing. The listener is checked before the launch with a probe
datagram carrying a probe token, so a blocked port is found before the
run, not after. If no datagram with the token has arrived by the
launch instant plus the delay plus `RuntimeMaxSec` plus `TimeoutStopSec`
plus 120 seconds, the read-out does not happen; a **recovery
procedure** does, recorded as such: one session opens, its own instant
and session id recorded as the recovery's, and it retains the unit's
state, the marker if any, the partial journal, and the driver's logs,
after which the record closes on the launch-or-signal failure with no
evaluation as a normal run, whatever the unit turns out to have done.

**Everything else is ADR 594's, unchanged**: the band and its facts, the
consistency contract, the completeness bounds, the start gate (package
at or below 45 °C, load below 0.3, read by the driver before any work of
its own), the perturbation gate, the 4% cutoff applied before the rule,
the four-term rule with `resolved`, `unresolved_tie_within_off_range`,
`below_floor_no_mechanism`, `withheld_instrument_incomplete` (a residual
that leads, at any size, or inconsistent facts),
`withheld_incomplete_profiles`, `withheld_unsupported_path`, and
`closed_on_variability`; the historical readings (ADR 593's 1,345.0 ms,
ADR 594's steady-pair band) beside the new one as context, never netted.

**One attempt, every outcome retained.** A gate refusal, a launch-
separation failure, an overrun, a variability closure, or any rule
outcome closes the record on that finding; no second attempt under this
number. Attribution only; no optimization.


## Acceptance

- The driver accepts `ADR595_LAUNCHER_SESSION_ID`, `ADR595_BOOT_ID`,
  `ADR595_ATTEMPT_TOKEN`, and `ADR595_LAUNCH_UNIX_MS`, stamps them on the
  summary with the timer's actual trigger it reads at start, refuses a
  differing boot id and a late start or a late trigger (each beyond
  30 s), collects the journal from the launch instant, and reports
  `launch_separation` (`verified` / `failed` / `unknown`, with the
  launcher's logout timestamp and the first child's launch timestamp)
  and `measurement_window` (`verified` / `failed` / `unknown`, with the
  session events joined to their children). Fixtures: the logout before
  the first launch (verified), inside the window (failed), after it
  (failed), absent from a collected collection (failed), outside the
  collected bounds (unknown), and under each of `truncated`, `degraded`,
  and `unavailable` (unknown); a window with no session event
  (verified) and with one (failed); and each collection state's effect
  on the window verdict.
- The owner is withheld with the named status whenever either interval
  is not `verified`, while the four-term table, facts, readings, and
  cutoff verdict are still reported; fixtures for each withholding.
- The completion hook's record (token, result, codes) is written beside
  the output directory and delivered as a datagram; the read-out waits
  for the token and a fixture of the wait rejects a stale or foreign
  token; the recovery procedure is a separate, recorded path.
- The results document quotes the launcher session id and boot id, the
  attempt token, the launch instant, the delay, the timer's scheduled
  activation and actual trigger, the launcher's logout timestamp, the
  first child's launch timestamp, the unit's timer accuracy, type, and
  timeouts, the completion record and the instant the datagram arrived,
  the read-out time, and the measurement window's session-event list,
  beside the host record and the evidence hashes.
- One run: eleven rows admitted, readings present, the six identities
  reconciled, the band's facts consistent, the cutoff verdict, and the
  four-term table with the rule's outcome or the closure; or the retained
  refusal, with its cause named.
- Plan commit, then one implementation/results/closure commit.


## Guardrails

- The band, the coordinate, the gates, the cutoff, and the rule are not
  changed under this record; only the launch and read-out procedure is.
- The read-out opens only on this attempt's completion datagram; elapsed
  time alone triggers the recovery procedure, never the read-out; the
  driver's own journal collection is the evidence for both intervals,
  not a later query.
- Build on bigfoot, measure on strix, nothing else on strix in the
  window; the workstation waits without connecting.


## Risks

- The unit may fail to start at its scheduled time (a user-manager
  restart, a timer skipped) or start late; the driver refuses a late
  start, the read-out finds no marker or a refusal, and the record closes
  on that finding with the scheduler's journal entries retained.
- A steady run under this procedure does not establish that ADR 594's
  spread came from its session events; a spread under this procedure
  does not establish that it did not. Either is reported as what it is.


## Forward

- If the rule resolves an owner, its record follows (ADR 586's pattern on
  the build write; pairwise pricing of the sizing passes for a
  single-pass candidate; or the instrument). If the cutoff closes the
  record again with the window clean, the next record is about the host,
  not q21, and starts from continuous sampling from outside the affinity
  set.
- The separated launch becomes the standard procedure for every strix
  measurement from here.
