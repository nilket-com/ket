# ADR 598 - strix's start gate: the launcher's hash verification and the gate's placement

**Status:** PROPOSED. Successor to ADR 597, which closed on ADR 593's
start gate without a measurement: two launches refused at 47 and 48 °C
against the 45 °C threshold, on a host reading 39–46 °C idle around
them.


## Context

ADR 593 placed the start gate — package temperature at or below
45,000 millic, one-minute load below 0.3 — at the driver's start, before
any work of the driver's own, after finding that the dataset's full-hash
verification alone warmed a P-core by ten degrees or more when the gate
was read after it. ADR 595 and ADR 596 passed that gate in the morning
at 44 and 45 °C. ADR 597's two afternoon launches were refused at 47 and
48 °C, with the package at 39–43 °C in the two minutes before the first
and 40–46 °C in the two minutes after the second, at load 0.00.

What runs between the timer's trigger and the gate's read, at
`6eaed138`. `toron-bench run` (`src/bin/toron-bench.rs`, the `run` verb)
canonicalises and reads the build manifest, then
`verify_staged_executable` on the harness (37.4 MB: canonicalise the
path, check the root and the filename, `sha256_file`, check
read-only), on the Toron executable when it differs (it does not on
these manifests), and on the staged DuckDB (61.9 MB), then execs the
harness. About 99 MB of SHA-256 on one thread, immediately before the
driver starts. The driver then reads its scope (`observe_driver_scope`:
cgroup, session, unit settings through `systemctl show`), the launch
environment (`adr595_launch_from_environment`: `loginctl`, the timer's
`LastTriggerUSec` through `systemctl --user show`), and only then the
gate (`adr593_start_gate`). Whether the hash, or the process spawns
that follow it, lifts the package sensor by the few degrees that
separate idle from the threshold at the instant the gate reads is a
hypothesis. ADR 597's results state it as one. This record tests it.

The threshold is not in question. 45 °C was declared under ADR 593 and
every strix record since has run under it; a gate that is read a few
seconds later or earlier is a placement, not a relaxation, and a
placement is what this record may change — after a measurement, with
the interval declared before the attempt that uses it.


## Decision

**Instrument the interval first.** The driver stamps, on every summary
and on every refusal, readings taken by `host_readings::take_reading`
(package temperature, the masked CPUs' frequencies, the thread's
context switches, the wall instant): `at_entry`, the driver's first act
before the scope and the launch environment are read, and `at_gate`,
the one reading the gate is judged on. `toron-bench run` records its
verification into the harness's environment —
`TORON_BENCH_VERIFY_BYTES`, `TORON_BENCH_VERIFY_MICROS`,
`TORON_BENCH_VERIFY_UNIX_MS_END` — and the driver stamps them as
`launcher_verification` with three states: `recorded` (all three
present and parsed, the values beside it), `absent` (none present: the
driver was not launched through `toron-bench run`, or by a `run` older
than this record; absence is not zero work), and `malformed` (some
present or unparsable, the raw values beside it). From `recorded`
stamps the summary carries `hash_end_to_gate_ms`, the delay from the
hash's end to `at_gate`'s instant — the gate-relevant interval, on
every run from here. A refusal writes `refusal.json` — the refusal's
text, the readings, the verification stamps — into the output
directory; to make that safe the driver's freshness check moves ahead
of the gate: the output directory must not exist, it is created, and
only then is anything written into it; an existing directory is refused
before any file is touched, as today, and the refusal of a non-fresh
directory is written nowhere but stderr. Nothing overwrites prior
evidence.

**Then a paired probe, under the separated launch, without q21.** A
verb `adr598-gate-probe`, run as one transient unit exactly as ADR 595
launches the drivers (the same scope and delay, one attempt), first
calibrates, then runs eight pairs of two trials, a **hash trial** and
a **no-hash trial**, the order alternating pair by pair (hash first on
even pairs, no-hash first on odd), with the same reading schedule in
both.

*Placement.* `toron-bench run` hashes under the unit's own mask —
`CPUAffinity` is empty on the unit, so every online CPU, P-cores and
E-cores — because the harness applies `TORON_TPCH_CPU_AFFINITY` at its
own start and the launcher is not the harness. The probe hashes under
that same mask: before each hash trial's work it sets its affinity to
the full online set, hashes, and restores the driver's mask (`0-15`)
before the pre-gate steps and the readings, which run under the pin as
the driver's do; the mask in force at the hash is recorded on every
trial. A probe that hashed inside the pinned driver would test a
different placement, and would say so if it did.

*Calibration, before the pairs.* Three hash trials with the full
schedule below, separated by the same 60 s gaps, whose durations are
retained (`calibration`) and whose readings are reported but enter no
excess; the **control duration** for every no-hash trial is the median
of the three, fixed before the first pair and not revised. The
calibration's baseline and observation run as a trial's do, so the
host settles for the same 60 s before the first pair.

*Each trial:*

1. a 60 s idle gap;
2. a baseline: the package temperature read once per second for 10 s
   (nominal offsets −10 … −1 s; actual instants recorded);
3. the work: on a hash trial, `verify_staged_executable` on the staged
   harness and DuckDB exactly as `toron-bench run` does (the same
   function, the same ~99 MB), under the launcher's mask; on a no-hash
   trial, a sleep of the control duration;
4. the driver's observational pre-gate operations, run as the driver
   runs them and timed — `observe_driver_scope`, then the *reads* the
   launch environment takes (`loginctl`, the timer's `LastTriggerUSec`
   through `systemctl --user show`) — and **not** the launch-admission
   check itself, whose late-start rule would reject every trial after
   the first; then, immediately, a dedicated reading named
   `at_gate_delay`, taken as the gate's own reading would be, stamped
   with its actual offset from the work's end (which may be tens of
   milliseconds, not a scheduled second);
5. the fixed recovery schedule: readings at nominal +0, +1, +2, +3, +5,
   +10, +20, +30, +45, +60 s after the work's end, each stamped with
   its nominal and its actual offset; a reading whose actual offset is
   more than 500 ms past its nominal is `late`, and a reading that
   could not be taken is `missing`; both are reported as such, never
   substituted.

*Per trial the probe reports:* the baseline's median and its readings;
the work's kind, mask, and duration; the pre-gate operations'
duration; `at_gate_delay` with its actual offset and value; the peak of
the schedule after the work and its offset; and **recovery**, defined
relative to the peak: the actual offset of the *third* of three
consecutive scheduled readings after the peak that all lie within 1 °C
of the baseline's median, or `unresolved` when any scheduled reading is
`missing`, or `late`, or no such run of three exists by +60 s.

*Per pair the probe reports* the raw temperatures on both trials at
every point, and the **baseline-adjusted excess**, the primary quantity:

```
excess_at_gate = (hash at_gate_delay − hash baseline median)
               − (no-hash at_gate_delay − no-hash baseline median)
```

and the same adjusted excess at the peak and at each scheduled offset.
A pair is **valid** when both of its trials have a baseline of ten
readings, an `at_gate_delay` reading, and no `missing` or `late`
scheduled reading; otherwise it is `invalid` with the reason.

*Across pairs:* the count of valid pairs; the count of valid pairs with
`excess_at_gate > 0`; the median `excess_at_gate` over the valid pairs;
the medians of the adjusted excesses at each offset; and recovery
aggregated explicitly — the hash trials' recoveries listed, the number
`unresolved`, and a median taken only when every hash trial of a valid
pair resolved (otherwise `median_recovery = unresolved`, with the
resolved values beside it, never a median over the resolved subset
alone). The threshold is not read and nothing is gated on it: the
probe measures the host.

**Lifetime, declared.** Three calibration trials and sixteen paired
trials, each about 10 s baseline, about 1 s work, a few seconds of
pre-gate operations, 60 s of observation, and a 60 s gap: about
2 min 20 s each, about 44 minutes in all. The unit is created with
`RuntimeMaxSec=3600` and `TimeoutStopSec=30`, the completion hook and
datagram as ADR 595's, and the workstation's listener deadline is the
launch instant plus the 120 s delay plus 3,600 plus 30 plus 120 s. A
unit stopped at the limit leaves the trials it completed in the
report, marked `stopped_at_runtime_limit`; such a report is descriptive
and decides nothing.

**What the probe decides, stated before it runs — and what it cannot.**
The decision needs complete evidence: **all eight pairs valid**, the
run not stopped. On that evidence, the hash is established as lifting
the sensor at the gate's read — *this probe's measured temperature
effect, nothing more* — if `excess_at_gate > 0` in at least six of the
eight pairs and the median `excess_at_gate` is at least 2 °C. Fewer
than eight valid pairs, or a stopped run, is `undecided`: the report
stands as description and no settle may be chosen from it. If
established, the successor attempt of ADR 597's experiment may declare
a settle interval — the median hash-trial recovery (as aggregated
above, the third confirming reading's offset) rounded up to the next
5 s, at most 30 s; a `median_recovery` that is `unresolved` or above
30 s declares none — as an operational choice, not a guarantee: the
gate is then read once, after the sleep, and that single reading is
`at_gate`, judged against the unchanged 45,000 millic; `at_entry` stays
the reading before the sleep, and no other reading decides. If the
hash is not established, the gate stays where it is and the successor
attempt makes no settle. **Neither outcome says why ADR 597's launches
were refused**: a negative result is consistent with ambient heat, with
the pre-gate operations, or with variation the probe did not control,
and a slow or unresolved recovery says the sensor moved, not what moved
it. The record reports the excesses, the recoveries, and the counts,
and the successor's plan declares its interval, if any, before its
launch. The threshold, the load bound, and the gate's refusal semantics
are unchanged.

**What this record does not do.** It does not relaunch ADR 597's
experiment; that is a successor record with its own plan commit,
carrying ADR 597's executables' identities or rebuilding from its
tree, and any settle procedure lands there, not here. It does not
change the threshold, the load bound, the readings' scope, or the
separated launch. It does not read the probe's readings as a gate, and
it does not attribute the refusals.


## Acceptance

- `at_entry` and `at_gate` and the `launcher_verification` stamps
  (`recorded` / `absent` / `malformed`, with `hash_end_to_gate_ms` under
  `recorded`) on every driver summary and on `refusal.json`; fixtures
  for each state, for the readings' order, for a refusal writing the
  file only into a directory the driver created, and for a non-fresh
  directory refused with nothing written.
- `adr598-gate-probe`: the calibration fixing one control duration
  from three retained hash timings; the paired procedure with the
  alternating order and the identical schedule; the launcher's mask at
  the hash recorded and the driver's restored after it; the
  observational pre-gate operations timed and the launch-admission
  check not run; the dedicated `at_gate_delay` reading with its actual
  offset; nominal and actual offsets on every scheduled reading, `late`
  and `missing` named; recovery as the third confirming reading;
  baseline-adjusted excess beside the raw temperatures; pair validity;
  explicit recovery aggregation; `stopped_at_runtime_limit`. Fixtures
  over injected readings: established (eight valid pairs, six positive,
  median ≥ 2 °C); not established (five positive; median under 2 °C);
  `undecided` (seven valid pairs; a stopped run); a `late` and a
  `missing` reading invalidating their pair; an `unresolved` recovery
  making `median_recovery` unresolved with the resolved values listed;
  the settle interval's rounding and its 30 s cap; and the hash's mask.
- The unit's `RuntimeMaxSec=3600`, the calibration's and the pairs'
  timings, and the listener's deadline stated in the launch record.
- One probe run on strix under the separated launch, its report
  retained, the decision read from it by the rule above, and the
  successor's settle interval named or declined.
- Plan commit, then one implementation/results/closure commit.


## Guardrails

- No q21 run, no measurement of Toron, no change to the threshold, the
  load bound, or the separated launch.
- Build on bigfoot, measure on strix, nothing else on strix in the
  window.
- Any settle procedure is implemented and used only under the
  separately approved successor attempt.


## Risks

- The probe's reads are work too; the paired design charges the same
  reads to both trials, which controls for the reads, not for what the
  afternoon's ambient does between pairs; the baseline adjustment
  controls for where each trial started, not for drift inside a trial.
- Nineteen trials at one afternoon's ambient say what that afternoon's
  host does; a morning may differ, and the record says so.
- The no-hash trial's sleep stands in for the hash's duration, not for
  the process activity `toron-bench run` performs around it (the
  manifest read, the canonicalisations); those are small and are named
  as uncontrolled.
- A dedicated `at_gate_delay` reading is one more read than the driver
  takes; it is the same read the gate takes, at the same point.


## Forward

- The successor attempt of ADR 597's experiment, under this record's
  decision, with the retention rule and the twelve-row contract as ADR
  597 fixed them.
- The legacy-index candidate, its own record.
