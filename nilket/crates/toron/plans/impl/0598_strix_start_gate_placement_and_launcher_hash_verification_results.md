# ADR 598 - strix's start gate: the launcher's hash verification and the gate's placement: results

Probed 2026-09-16 on strix under ADR 595's separated launch, without a
q21 run and without touching the threshold. **Decision, by the rule
declared before the probe ran: `not_established`.** Of the eight valid
pairs, four had a positive baseline-adjusted excess at the gate's delay
(the rule needs six), and the median adjusted excess was 1.75 °C (the
rule needs 2 °C). The gate stays where it is; the successor attempt of
ADR 597's experiment declares no settle interval from this record.

What the probe observed beyond its question, reported as observation
and not as a decision, on these nineteen trials: about 8 ms after the
work's end the package sensor read above the trial's baseline on
**both** kinds of trial — the hash trials by 3–10 °C (median 7.5), the
no-hash trials by 4–8 °C (median 5.25) — and in every trial but one
it was back within the baseline's band by the +2 s reading. The paired,
baseline-adjusted excess of the hash trials over the no-hash trials at
that instant was a median +1.75 °C. What raised the reading on the
no-hash trials — a 42 ms sleep under the launcher's mask, the affinity
transitions, the driver's ~8 ms of pre-gate operations, the readings
themselves — the probe did not separate, and this record does not
assign.

## Identity

| identity | value |
|---|---|
| probe executable | `6408b52a5faf928cf93fed5b3ad88e4a11ef7030fd48077d0f33687810bfc65d`, tree `152a56fa3ba4369331e15825569639243787c553`, commit `0c3c522d` (the closing commit differs in this document and the ADR's status), manifest `evidence/build/adr598-probe.manifest` |
| launcher | `toron-bench` built from the same tree on bigfoot, `23267312cc764392…`, installed on strix in place of the previous launcher (retained as `toron-bench.pre-adr598`); it hashed 99,388,424 bytes in 45,785 µs under mask `0-31`, read back by the probe |
| recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580` |
| unit | `adr598-gate-probe.service` under `user@1002.service/app.slice`, `Linger=yes`; `Type=exec`, `RuntimeMaxSec=3600`, `TimeoutStopSec=30`, `KillMode=control-group`; timer `OnActiveSec=120`, `AccuracySec=1s`; listener deadline launch + 120 + 3,600 + 30 + 120 s |
| masks | launcher `0-31` (the unit's, every online CPU), driver `0-15` (the harness's pin); the probe hashed and slept under `0-31` and read under `0-15`, the same transitions on both trial kinds, the mask in force at the work recorded on every trial |
| build host / run host | bigfoot / strix |

## The launch

| instant (CDT) | what |
|---|---|
| 15:47:49 | preflight: the completion hook's datagram from strix reached the workstation's listener (18 ms) |
| 15:47:55.978 | `ADR595_LAUNCH_UNIX_MS`, launch session 2296, token `6cf6b4819ebb8c973adaf0e1`, delay 120 s; the session exited |
| 15:49:56 | timer trigger (`LastTriggerUSec`, second resolution) |
| 15:49:56.955 | the launcher's hash ended (`TORON_BENCH_VERIFY_UNIX_MS_END`) |
| 15:49:56.957 | the probe's `at_entry` reading, 2.1 ms after the hash's end: **49 °C**, one P-core at 5.70 GHz; the driver's scope and launch environment read after it, separation 121.0 s (the probe's own admission check, run once at its start) |
| 15:56:27 | first pair's first trial started (after three calibration trials) |
| 16:30:07.825 | completion hook: `result=success exit_code=exited exit_status=0`; datagram received at +14 ms |
| 16:30:39 | read-out session 2299; unit and timer inactive; package 41 °C at idle |

Evidence on strix: `target/tpch/adr598-gate-probe/probe.json`
(`e463227e0f70a2b54…`, `complete: true`; written by the probe as first
cut, before the review-round fixes below, and preserved unchanged) and
`target/tpch/adr598-strix-host-record/` (`host_config_before.txt`
`6a623902…`, `host_after.txt` `5cfd21e2…`, `completion.marker`
`0e498f53…`, `driver_stdout.log` `e13532a2…`, `launch.sh` `5b98bf49…`,
`snapshot.sh` `c9480d99…`, `tool_versions_run_host.txt` `fddfba2d…`).

## Calibration

Three hash trials: 42,339, 42,444, and 42,372 µs for the 99.4 MB;
**control duration 42,372 µs**, fixed before the first pair. Their
readings (baseline 39–42 °C after a 49 °C first reading on the first
trial, still cooling from the driver's start; gate delay 44, 46, 46 °C
at 8.1–8.4 ms; back to 39–40 °C by +2 s) entered no excess.

## The pairs

All eight pairs valid: ten baseline readings and an `at_gate_delay`
reading on every trial, no `missing` and no `late` scheduled reading
(the scheduled readings' actual offsets within 0.3–0.9 ms of nominal).
Hash trials 42.1–44.1 ms; no-hash sleeps 42.6 ms; the pre-gate
operations 7.7–8.3 ms on both kinds; `at_gate_delay` at 7.9–8.7 ms
after the work's end on every trial. **A limit of the retained
artifact:** the probe as first cut reconstructed the baseline readings'
`actual_offset_ms` from the nominal schedule (`started + tick`) rather
than from the instants the readings were taken at, so the retained
`probe.json` cannot establish the baseline's actual cadence — only the
scheduled readings' offsets, which were measured, and the baseline
temperatures, which were. The decision reads the baseline's median
temperature and not its timing, so the decision is not affected; the
closing code captures the actual instants (`adr598_reading_now`,
`adr598_baseline_offsets`), and the artifact is preserved as written.

| pair | order | hash: base → gate (rise) | no-hash: base → gate (rise) | adjusted excess at gate | at +2 s (adj.) | hash recovery |
|---:|---|---|---|---:|---:|---:|
| 0 | H,N | 39 → 49 (+10) | 39 → 44 (+5) | **+5.0** | +4.0 | 45.0 s (see below) |
| 1 | N,H | 39 → 48 (+9) | 38.5 → 44 (+5.5) | **+3.5** | −0.5 | 5.0 s |
| 2 | H,N | 40 → 47 (+7) | 38 → 46 (+8) | −1.0 | −1.0 | 5.0 s |
| 3 | N,H | 38 → 46 (+8) | 38 → 42 (+4) | **+4.0** | −1.0 | 5.0 s |
| 4 | H,N | 38 → 44 (+6) | 38 → 44 (+6) | 0.0 | 0.0 | 5.0 s |
| 5 | N,H | 39 → 43 (+4) | 38 → 44 (+6) | −2.0 | −2.0 | 5.0 s |
| 6 | H,N | 38 → 41 (+3) | 38 → 42 (+4) | −1.0 | −1.0 | 5.0 s |
| 7 | N,H | 38 → 48 (+10) | 38 → 42 (+4) | **+6.0** | 0.0 | 5.0 s |

Temperatures in °C; adjusted excess = (hash gate − hash baseline
median) − (no-hash gate − no-hash baseline median). Positive pairs
**4 of 8**; median adjusted excess **+1.75 °C** (values −2.0, −1.0,
−1.0, 0.0, +3.5, +4.0, +5.0, +6.0); at the peak the same, since on
every trial but one the peak was the gate-delay reading itself (the
+0 and +1 s readings equal it, +2 s is back). Raw rises at the gate
delay: hash 3–10 °C, median 7.5; no-hash 4–8 °C, median 5.25. Hash
recoveries: 5.0 s on seven trials (the third confirming reading is the
+5 s reading, after +2 and +3 within the band) and 45.0 s on pair 0's;
`unresolved` 0; median 5,001 ms, aggregated over all eight — and not
used, since nothing was established.

Pair 0's hash trial read **81 °C at +5 s** — 37 °C over its baseline —
between a 42 °C reading at +3 s and a 41 °C one at +10 s, at
15:56:43; nothing in the probe accounts for it, the probe collected no
journal, and it is not attributed. It made that trial's recovery 45 s
and its adjusted excess at +5 s +43 °C; it did not touch the gate-delay
reading, which is the decision's.

## The decision, and what it does not say

`not_established`: four positive pairs against six required, a median
adjusted excess of 1.75 °C against 2 °C required. By the rule declared
in the plan, the gate stays at the driver's start and the successor
attempt of ADR 597's experiment makes no settle from this record.

Not decided, but on the record: on these trials the package sensor
read 3–10 °C over the trial's baseline about 8 ms after the work on
both arms and was back within the band by +2 s in every trial but one;
the probe's own entry reading, 2 ms after the launcher's hash, was
49 °C on a host whose trial baselines were 38–40 °C; ADR 597's gate
readings of 47 and 48 °C were taken at the same point of the same
procedure. That is consistent with the refusals and does not attribute
them: the probe did not vary the driver's start, did not separate the
no-hash trials' rise into its parts, and this record makes no claim
about what the refusals' extra degrees came from. A successor that
wants to place the gate at a different point declares the placement
and the hypothesis before its attempt, with the threshold as it is.

## Review round: three fixes, no second probe

Review found three defects in the first cut, fixed in the closing
commit without another run; the retained `probe.json` and the
host-record files are preserved as written. (1) A refusal at the gate
lost the gate's reading: `adr593_start_gate` returned only the text.
The gate now returns a structured outcome (`Adr593GateOutcome`: the
reading, the load, the bounds, `admitted`, the refusal), the driver
captures it as it is read, and `refusal.json` carries
`readings.at_gate`, `load_1m_at_gate`, `start_gate`, and
`hash_end_to_gate_ms` when the gate was read, and `gate_evidence:
absent: refused before the gate was read` with those fields explicitly
`null` when it was not; the driver's scope and gate are injectable
(`LifecycleHostProbes`), and fixtures drive a 47 °C refusal through the
driver and a scope refusal before the gate. (2) The baseline's actual
offsets were reconstructed from the nominal schedule; the probe now
records the instant each reading was taken (`adr598_reading_now`) and
converts those once the work's end is known (`adr598_baseline_offsets`),
with a fixture on a delayed read; the retained artifact's limit is
stated above. (3) An incomplete report was labelled
`stopped_at_runtime_limit` whatever the cause, while running and after
an ordinary failure alike. The report now carries `state` (`running` at
every checkpoint, `complete` at the end, `failed` with the error on an
ordinary failure, written before the error returns), the decision names
an incomplete run `undecided_incomplete`, and a stop at the unit's
runtime limit is asserted only by `adr598_run_disposition` from the
completion marker's `result=timeout` (`adr598-disposition --report
--completion-marker`), else `incomplete_unknown`; fixtures cover a
checkpoint, an ordinary failure, a timeout, a success marker on a
running report, and a report from before the field. The retained
report's disposition under the closing code is `complete`.

## Implementation and verification

Launcher (`toron-bench run`): `TORON_BENCH_VERIFY_BYTES`, `_MICROS`,
`_UNIX_MS_END`, and `TORON_BENCH_LAUNCHER_CPU_MASK` (the launcher's
allowed mask, read from `sched_getaffinity`) handed to the harness.
Driver (`run_lifecycle_decomposition_driver`): `at_entry` as the first
act; the output directory's freshness decided and the directory created
before anything else can refuse; `refusal.json` (the refusal, the
entry reading, the verification stamps) written only there, a
non-fresh directory refused with nothing written; `launcher_verification`
as `recorded` / `absent` / `malformed`, `hash_end_to_gate_ms` under
`recorded`; `readings.at_gate` is the gate's own reading. Probe
(`adr598-gate-probe`): `adr598_host` reading the launcher's mask back
(refusing without it), the manifest, and the scope; the calibration;
`adr598_run_trial` with the identical affinity transitions, the
observational pre-gate operations (`observe_driver_scope`, the timer's
read through `adr595_read_timer_last_trigger`, factored out of the
admission check), the dedicated `at_gate_delay` reading, and the fixed
schedule with nominal and actual offsets; `adr598_derive`, `adr598_pair`,
`adr598_decide` pure over the readings; `probe.json` rewritten after
every trial with its `state`, `complete` only at the end;
`adr598_run_disposition` and the `adr598-disposition` verb. Fixtures:
the verification stamps in each state; the refusal file written into a
created directory and a non-fresh directory left untouched (run
against the ADR 596 driver under the test's session scope); a 47 °C
gate refusal keeping the gate's reading and a scope refusal saying the
gate was not read; the baseline offsets from actual instants with a
delayed read; the disposition from state and marker; a trial's peak and
its third confirming reading, an unresolved recovery, a late reading, a missing
reading, a short baseline, a missing gate reading; a pair's adjusted
excess beside its raw temperatures and an invalid trial invalidating
the pair; the decision established with the settle rounded to 5 s,
undecided on seven pairs and on a stopped run, not established on five
positive pairs and on a median under 2 °C, an unresolved recovery
making the median unresolved and the settle none, a recovery past 30 s
giving no settle, and 30 s admitted.

Ladder on bigfoot (`RUST_MIN_STACK=67108864`, fmt clean): Toron
library 1,817, architecture 2,151, integration 3,649; toron-tpch
library 241 (+4 ignored), `toron-tpch` binary 126, `toron-bench` 9,
integration 261 (+2 ignored) at the probe's tree; the closing tree
re-run in full.

## Closure

ADR 598 closes `not_established`: the launcher's hash is not shown to
be what lifts the gate's reading, and no settle interval is declared
for the successor attempt. What the probe put on the record is what
the sensor read at the gate's delay on these trials — 3–10 °C over the
baseline on both arms about 8 ms after the work, back within the band
by +2 s in every trial but one — with the hash trials' paired excess
over the no-hash trials under the bar. The gate, the threshold, the
load bound, and the separated launch are as they were; the driver now
stamps its entry reading, the launcher's verification, and its
refusals with the gate's reading when it was read.
