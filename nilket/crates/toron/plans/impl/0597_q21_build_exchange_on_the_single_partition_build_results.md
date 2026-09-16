# ADR 597 - q21's replay-1 build write: the Exchange feeding the single-partition build: results

Closed 2026-09-16 without a measurement. The experiment as ADR 597 fixed
it — two release executables one call site apart, the twelve-row paired
run under ADR 595's separated launch, ADR 588's retention rule — was
built, validated on bigfoot, staged and hash-verified on strix, and
launched once. **ADR 593's start gate refused the driver before any
child ran: the package temperature at the driver's start was 47 °C
against the gate's 45 °C.** Under the plan, a gate refusal is a
non-retained outcome under one attempt, so the record closes on it: the
treatment call site is reverted, the parameter, the admission, the
facts, the fixtures, and the driver stay, and **no performance evidence
was obtained — the candidate is unpriced, not without benefit.** A
second launch was made before the closure was called and is recorded
below as a protocol deviation; it was refused at the same gate (48 °C)
and produced nothing either.

## Identity

| identity | value |
|---|---|
| control executable | `bebcf790261ffc5a95cbbb4c2983d9d7dd7429fa4e2e47b43ba2df1225ed8c05`, tree `44455a687b3e4f125a63f84d07298428b5550b4e` (the record's implementation with the call site at `Split`), manifest `evidence/build/adr597-control.manifest` (`2206c788…`) |
| treatment executable | `64c2e7133973508dde40ce2634d93fd30a970bddf060a254a81e2fbe2915c250`, tree `1917d0d6a64ba468e2520ea711aad7aafb2e8ff1` (the control tree plus the one call site at `PassThrough`; six lines of diff, three of them comment), manifest `evidence/build/adr597-treatment.manifest` (`4abea398…`) |
| recipe | `9af51cbcec10b54da8dfc14cd5f9d343b0b8fabab636896c390ae2c11a7d5580`, both arms; ADR 588's manifest gate would admit the pair (same recipe, different trees, different executables, no untracked inputs) |
| closing tree | the control tree's code with this document and the ADR's status; the treatment call site is not in it |
| sealed baseline (bound by content) | `950ebc4b…` on tree `a962f3d8…`, artifact `d03935eb…`, q21 median 9,126.9 ms, affinity `0-15`, dataset `fnv1a64:0fc5c06d19c5ed23` |
| driver unit | `adr597-exchange-skip.service` under `user@1002.service/app.slice`, `Linger=yes`; `Type=exec`, `RuntimeMaxSec=10min`, `TimeoutStopSec=30s`, `KillMode=control-group`; timer `OnActiveSec=120`, `AccuracySec=1s`; the treatment executable launched through `toron-bench run` with `--control-manifest` naming the control |
| build host / run host | bigfoot / strix |

## The attempt, and the deviation

| instant (CDT) | what |
|---|---|
| 14:58:53 | preflight: the completion hook's datagram from strix reached the workstation's listener (35 ms) |
| 14:59:00.412 | attempt 1: `ADR595_LAUNCH_UNIX_MS` 1789588740412, launch session 2278, token `e9bf1e934d6da814ed14e11c`, delay 120 s; the session exited |
| 15:01:00 | the unit started; the driver read the gate — **package 47,000 millic, above 45,000** — and refused (exit 2); no child launched; `completion.marker`: `result=exit-code exit_code=exited exit_status=2` at 1789588860980, datagram received at +37 ms |
| 15:01:12 | read-out session 2279; unit and timer inactive; no output directory |
| 15:01:34–15:03:14 | idle samples: package 39–43 °C, load 0.00 |
| 15:04:05.370 | **attempt 2, a protocol deviation**: launched before the closure was called, session 2282, token `ba62db838e6d049b8e0e9792` |
| 15:06:05 | the unit started; gate read **48,000 millic**; refused; no child |
| 15:06:17 | read-out session 2283; package 46 °C at that instant, then 40–45 °C over the next two minutes at load 0.00 |

Both attempts' artifacts are retained under
`target/tpch/adr597-strix-host-record/attempt1-gate-refused/`
(`completion.marker` `64d288ea…`, `driver_stderr.log` `4021bf92…`,
`host_config_before.txt` `38969a43…`, `host_after_attempt1.txt`
`c0520591…`) and `attempt2-gate-refused/` (`completion.marker`
`94e5f72c…`, `driver_stderr.log` `32fedb3c…`, `host_config_before.txt`
`045c8400…`), beside `launch.sh` (`147e6821…`), `snapshot.sh`
(`4c399531…`), and `tool_versions_run_host.txt` (`d88e2dc9…`). The
user manager's journal records both units' start and exit
(`status=2/INVALIDARGUMENT`) at 15:01:00 and 15:06:05.

**What closes the record.** Attempt 1's gate refusal. The plan fixed
one attempt and named a gate refusal among the outcomes that revert the
call site; a refusal before the first child is not a measurement, but
it is the attempt. Attempt 2 was launched on the reasoning that a gate
refusal leaves the attempt unmade, as ADR 593's relaunch after moving
its gate had done; that relaunch was a disclosed deviation and not an
exemption, and so is this one. It changed nothing: it was refused at
the same gate, no child ran under it, and it is not the basis of any
statement here.

**What the refusals say and do not say.** The gate was read at the
driver's start on both attempts and found the package at 47 and 48 °C;
in the idle minutes around them the package read 39–46 °C; ADR 595 and
ADR 596 had read 44 and 45 °C at the same gate in the morning. The
driver starts immediately after `toron-bench run` verifies the staged
treatment executable's hash, which is one core's work over about a
hundred megabytes; whether that lifts the package by the few degrees
that separate idle from the gate is a hypothesis this record does not
test. It is the successor's question, and the temperature threshold is
not this record's to move.

## What was built and validated

Engine: `ExchangeHandling { Split, PassThrough }` on
`write_streaming_scan_inner_build_batches`; `exchange_pass_through_refusal`
admitting pass-through only for the single-file branch with an Exchange
of at least one partition whose key program is a single plain-column
root of type `Int32` or `Int64` inside the scan's schema, refusing
everything else by name (`branch_not_admitted`, `zero_partition_count`,
`multi_root_key`, `computed_key`, `key_ordinal_out_of_schema`,
`key_type_not_fixed_width`) and falling back to the split, errors
included; the ledger facts `exchange_handling`,
`pass_through_requested`, `pass_through_refusal`,
`scratch_plaintext_capacity_peak`, `scratch_refs_capacity_peak`,
`writer_peak_bytes`, `resident_state_peak_bytes`, and
`scratch_releases`, the peaks observed as the write guard is about to
read them; the receipt band's rules for them (`build_write_facts_vintage`
naming a receipt recorded before these facts existed and reading it
under ADR 596's rules alone); every caller of the writer passing
`Split`, the spilled branch included in the closing tree.

The memory grounding was corrected by the fixture. The plan wrote that
a whole-batch page the split's pages fit under would be refused at the
write's guard. The guard does not refuse on a page's size:
`guard_resident_state_after_write` releases the scratch — both buffers
dropped, the plaintext high-water mark reset — when the resident total
is over the cap, then re-checks the retained state alone (index bytes,
the 64 KB reserve, baseline, pinned bytes), and refuses only on that.
The fixture asserts what the code does: with variable-width,
incompressible rows (380–398 bytes each, 2,000 rows), the split's four
pages of about 200 KB pass with the scratch retained and no release;
the whole-batch page of about 800 KB is admitted after one release, the
ledger having recorded the transient peak the guard read
(`writer_peak_bytes` over 600 KB, `resident_state_peak_bytes` over the
cap, `scratch_releases` 1) and the scratch standing at zero capacity
afterwards; and a cap the retained state alone exceeds refuses the first
page on both paths identically — `BudgetDimension::Memory`, the write
guard's site, `writer_peak_bytes=0` in the detail because the release
came first — after the page was written and pushed, the error
propagating out of the writer, the branch's cleanup removing the file.

Value parity through the build's own `lookup` and page reader: three
batches with unique keys, keys repeated within and across batches, and
null keys, written under `Split` (an Exchange of four) and
`PassThrough`; every distinct key (13, or 14 with the null under a
null-safe plan) returns the same complete rows — every column, as a
multiset — on both builds, an absent key returns nothing on both, the
null rows are written but not indexed under the non-null-safe plan on
both; the split writes more pages than pass-through's one per batch.
Refusals: the computed key, the two-root key, the ordinal past the
schema, the `Utf8` key, and the zero partition count each take the
split with the control's outcome (the same rows, pages, and bytes, or
the same error where the split's evaluation fails) and record the
reason; the null-carrying `Int64` key is admitted.

Harness: `LifecycleArms::TwoExecutables` on the lifecycle driver
(`adr597-q21-exchange-skip`): the control loaded from
`--control-manifest` and the treatment from the launching manifest,
hash-verified and gated as ADR 588 gates them; the twelve-row contract
(`adr597_run_children`: the control's warmup, the treatment's, then five
alternating pairs, every row stamped with its arm's executable and
validated through ADR 592's validator as it returns, refusals journaled
and the run stopped); `evaluate_adr597_pairs` with ADR 588's verdict on
the `verdict` field; the cutoff and ADR 595's precedence generalised to
that field; `adr597-reevaluate` with the row contract
(`lifecycle_window_from_rows`) requiring the twelve rows in their order
with every stamp. Fixtures: a complete twelve-row run in order with the
arm's executable on every row and the journal satisfying the contract;
a failing warmup on either arm stopping the run with its refusal
journaled (one launch, two launches); a pair row from the wrong
executable refused where it returns; `retained`, `not_established` (a
zero pair; a median under the bar), a wrong order and a warmup among
the pair rows refused; the cutoff and the precedence on the verdict
field; the contract refusing eleven rows, swapped warmups, a wrong
position, a late warmup, a missing first launch, a missing last return.

Ladder on bigfoot (`RUST_MIN_STACK=67108864`, fmt clean), on the control
tree and on the treatment tree alike: Toron library 1,817, architecture
2,151, integration 3,649; toron-tpch library 241 (+4 ignored),
`toron-tpch` binary 121, `toron-bench` 9, integration 261 (+2 ignored).

## Closure

ADR 597 closes on ADR 593's gate, not on its question. No pair ran, no
digest was compared on strix, no gain or loss was measured, no resource
observation was taken on either arm: the Exchange-skip candidate is
exactly as unpriced as ADR 596 left it, with its value-equivalence
argument now a fixture and its memory accounting corrected to what the
guard does. The treatment call site is reverted in the closing tree;
the parameter, the admission, the facts, the fixtures, and the
two-executable driver remain for the attempt that clears the gate. That
attempt is not this record's: the successor examines the launcher's
hash verification and the gate's placement, declares any settle
interval before it launches, and keeps the threshold; the legacy-index
candidate stays a separate record.
