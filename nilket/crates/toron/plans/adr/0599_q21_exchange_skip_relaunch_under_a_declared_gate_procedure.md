# ADR 599 - q21's replay-1 build write: the Exchange-skip experiment relaunched under a declared gate procedure

**Status:** PROPOSED. Successor to ADR 597, which built and validated
the write-only Exchange-skip experiment and closed on ADR 593's start
gate without a measurement (candidate unpriced), and to ADR 598, which
probed the gate's read and established nothing about the launcher's
hash (no settle authorised, no gate change).


## Context

ADR 597 left a validated experiment unrun: `ExchangeHandling` on the
spilled build's writer with `PassThrough` admitted for a single
plain-column `Int32`/`Int64` key on the single-file branch, the ledger
facts and the receipt band's rules, the value-parity and memory
fixtures, the two-executable lifecycle driver
(`adr597-q21-exchange-skip`) with its twelve-row contract, ADR 588's
manifest gate and retention rule, and `adr597-reevaluate`. Its two
launches were refused at the gate at 47 and 48 °C; the treatment call
site was reverted; nothing was measured. ADR 598 then observed, on
nineteen trials, that about 8 ms after a 42 ms burst of work the
package sensor read 3–10 °C over the trial's baseline on both a hash
and a no-hash arm, and was back within the baseline's band by the +2 s
reading in every trial but one; the probe's own entry reading, 2 ms
after the launcher's hash, was 49 °C on a host whose trial baselines
were 38–40 °C. ADR 598 decided nothing from that beyond
`not_established` for the hash, and this record does not read it as a
cause either: it is the motivation for a placement decision made and
justified here as an operational choice, whose effect the attempt
observes at its endpoints and does not establish.

The driver, since ADR 598, stamps `at_entry` (its first act),
`at_gate` (the reading the gate is judged on), the launcher's
verification and `hash_end_to_gate_ms`, and writes a refusal with the
gate's reading when the gate was read.


## Decision

**The same experiment, unchanged where it was validated.** The engine,
the admission, the facts, the fixtures, the driver, the twelve-row
contract, ADR 592's validator with the arm's executable on every row,
ADR 593's readings, journal, scope, and 4% cutoff on the control
range, ADR 595's separated launch and precedence, and **ADR 588's
retention rule — retained only if the median paired gain exceeds
`2 × range_control` and every pair is strictly positive** — are ADR
597's, by reference, and are not restated or revised here. The bar is
whatever the run's control range makes it, stated before the gain is
read.

**Rebuilt from the closing tree, one call site apart.** Two release
executables on one recipe: control from this record's tree with the
spilled branch at `Split` (as ADR 597 left it), treatment from that
tree plus the one call site at `PassThrough` — the same six-line
difference ADR 597 measured nothing with. Rebuilt rather than reused
because ADR 597's executables predate the ADR 598 driver stamps this
record relies on; ADR 597's identities (`bebcf790…` / `64c2e713…`) are
cited as the validated predecessors and the diff between the two new
trees is asserted to be the one call site.

**The gate procedure, declared here.** The threshold (45,000 millic),
the load bound (0.3), and the refusal semantics are ADR 593's. What
this record decides is *when* the one reading that decides is taken:
the driver takes `at_entry` as its first act, reads its scope and the
launch environment as today, then **waits 5 s doing nothing of its
own**, and then takes the gate's reading — `at_gate`, the single
reading judged against the threshold, with `gate_settle_seconds: 5`
stamped beside it. Rationale, and its limits: ADR 598 observed the
sensor back within the baseline's band by +2 s after a short burst in
eighteen of nineteen trials and at +5 s in all of them but one; 5 s is
the offset of the third confirming reading on seven of the eight hash
trials. That is why 5 s and not 0 or 30. It is not a claim that the
wait removes what refused ADR 597 — ADR 598 did not attribute those
readings — and the attempt cannot establish the placement's
effectiveness either: `at_entry` is taken before the scope and launch
reads, so it is not what the old placement's gate would have read, and
one run is one run. What the record reports is the two endpoint
observations and the verdict — the entry reading, the gate reading
after the wait, the load at the gate, and whether the gate admitted or
refused — in every combination they can take (entry below and gate
below, entry above and gate below, entry below and gate above, both
above; and a load refusal with the temperature under the threshold),
with causality unestablished in each. Admission is the gate's verdict
on temperature and load together, never the temperature alone.

**The wait itself, recorded and checked.** The setting is a driver
flag, `--gate-settle-seconds N`, default 0 (today's placement, used by
every other verb and record); this record passes 5. The driver records
`gate_settle`: the requested seconds, and the wait's actual start and
end on the monotonic clock and its elapsed duration, so a wait that
was skipped, cut short, or replaced by five seconds of other work is
visible as such. The order is fixed and tested: the launch
environment's admission, then the wait, then the gate's reading; the
gate's reading is the first act after the wait ends. A flag value
that is malformed, negative, missing its value, or larger than the
unit's runtime allows (the driver refuses anything over 600 s) is a
refusal, never a default to zero. A refusal before the wait (the
scope, the launch environment) records the requested setting with
`wait_performed: false` and the gate evidence absent, as ADR 598's
refusal file does; a refusal at the gate records the wait as
performed with its measured duration beside the gate's reading. The
setting is part of the run's declared coordinate and is stamped on
the summary and on any refusal.

**One attempt, no deviation.** One launch. A gate refusal, a validator
refusal, a variability closure, a withheld launch, or `not_established`
closes the record with the treatment call site reverted in the closing
commit, as ADR 597 fixed; a second launch is not made under this
record, whatever the refusal says. The mechanism check (ADR 596's
driver on the treatment, under the same gate procedure and one
attempt) follows a verdict — `retained` or `not_established` — and
never a refusal or closure, and never reopens the verdict.

**What this record does not do.** It does not touch the threshold, the
load bound, the sizing passes, the partitioned branch, the probe side,
the binder, or the fingerprint's scope; it does not price the legacy
index; it does not retain a gain the rule does not establish.


## Acceptance

- `--gate-settle-seconds` on the lifecycle driver: default 0; the wait
  placed after the launch environment's admission and before the
  gate's reading, its requested seconds and monotonic start, end, and
  elapsed recorded as `gate_settle`; `at_entry`, `at_gate`, and
  `gate_settle` on the summary and on a refusal. Fixtures: the default
  (no wait, `elapsed` zero, the gate read at once); a setting whose
  wait is measured (`elapsed` at least the requested seconds, the gate
  read after the wait's end, the order launch admission → wait → gate
  asserted through injected probes that record the sequence);
  malformed, negative, missing-value, and over-limit flag values each
  refused; a refusal before the wait recording the setting,
  `wait_performed: false`, and absent gate evidence; a refusal at the
  gate recording the performed wait beside the gate's reading.
- Two executables built on bigfoot from this record's tree, one call
  site apart (asserted by diff), both ladders green, both staged and
  hash-verified on strix; ADR 588's manifest gate admitting the pair.
- One run on strix under the separated launch with `--gate-settle-seconds 5`:
  twelve rows admitted, readings present, separation and window
  verified, the control range and the bar stated, the gain per pair and
  its median, the verdict; or the retained refusal or closure. Either
  way the results report the entry reading, the gate reading, the load
  at the gate, the measured wait, and the gate's verdict, as
  observations with causality unestablished.
- Plan commit, then one implementation/results/closure commit; every
  outcome other than `retained` reverts the call site in that commit.


## Guardrails

- The retention rule, the twelve-row contract, and the admission are
  ADR 597's verbatim; the threshold is ADR 593's.
- Build on bigfoot, measure on strix, the separated launch, nothing
  else on strix in the window; no relaunch under this record.


## Risks

- The wait may not suffice on a warm afternoon; the record then closes
  on the refusal with both readings on file, which is itself the next
  record's evidence about placement.
- The control range decides the bar; a range like ADR 596's puts it
  near 650 ms, and the record says `not_established` at that bar as a
  fact about the run.


## Forward

- Retained: the successor decides whether the sizing passes' and the
  partitioned branch's handling of the same Exchange follow, each
  under its own paired record.
- Not established or refused: the parameter, the driver, and the
  fixtures stay; the legacy-index candidate is its own record either
  way.
