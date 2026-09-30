# ADR 618 - SF32 scaling diagnostic

**Status:** PROPOSED. Successor to ADR 617. **A diagnostic record: no
token, no pair, no cut, no saving projected.** Sealed SF8 at ADR 613's
coordinate stays the campaign target. SF16 and SF32 artifacts are
unsealed diagnostics and never enter the SF8 board.

ADR 617 measured SF8 and SF16 at a fixed 2 GiB task memory, and observed
two transitions at SF16:
- q21's execution-stage topology gains a deferred, reservoir-buffered
  replay holding 1.85 GB of spill;
- q18's three-call join bucket starts spilling (421 MB).

q09's gap widened without an observed transition, and q01 and q05
approximately doubled. **SF32 adds a third point.** It shows:
- whether the SF16 transitions stay the same kind or change again;
- whether q09's widening continues, and whether a transition appears;
- whether q01 and q05 keep approximately doubling;
- how memory (q05's peak RSS reached 13.0 GB at SF16) behaves.

As in ADR 617, readings are candidates. Three points still do not
uniquely separate cost kinds.


## Context

**The data.** The SF32 strict dataset is generated on bigfoot with ADR
613's measured executable (`9a84d5c4…`), the same generator as SF8 and
SF16:
`toron-tpch prepare --data-source parquet --mode strict --sf 32`,
into `target/tpch/data/strict/sf32/`, the same absolute path on strix.
- The results record gives its manifest fingerprint, row counts, and
  sizes.
- It is copied to strix, and every file's SHA-256 and the byte total
  are compared across the hosts.

**Host budget.** strix has 123 GiB of memory and 2.5 TB free.
- The largest expected Toron RSS is q05's. SF16's 13.0 GB doubled gives
  about 26 GB, an extrapolation.
- q05's replay-1 build estimate doubled gives about 23.0 GB. That is
  below the 32 GiB cap that ADR 616's and ADR 617's receipts observed,
  so the estimate is expected to imply `in_memory`. The receipt reports
  it; the plan does not assume it.
- q21's SF16 spill was 1.85 GB. SF32's may be several times larger. The
  spill scratch is on the same filesystem.

**Duration, sized from ADR 617's own campaign log.** ADR 617's blocks
summed to 3,009 s:
- SF8: Toron 649 s, DuckDB 276 s;
- SF16: Toron 1,522 s, DuckDB 561 s.

Doubling SF16 gives roughly 4,200 s for SF32. q21's growth faster than
linear may add to that. That is an extrapolation used only to set the
unit's limit, not a prediction.


## Decision

*Cells, coordinate, and executable* are as in ADR 617:
- the cells q01, q05, q09, q18, and q21;
- strict parquet, task memory and spill reserve 2 GiB (fixed, so that
  budget boundaries show), partition parallelism 1, DuckDB threads 1,
  affinity 0-15;
- the executable built from **this record's plan commit**, with `src`
  verified identical to ADR 616's and ADR 617's (`3a782d97`). The build
  is expected to resolve to the same executable (`5eadef2d…`), and that
  is verified, not assumed.

*A same-session ladder: SF8, SF16, and SF32 in one campaign.* ADR 617's
SF8 and SF16 figures are from a different session, so all three scales
are re-measured together. **ADR 617's figures are context only.**

*The timed campaign.* ADR 617's runner and row contract, unchanged
except for the scales:
- one `perf --runs 1 --warmup 1` block per cell, engine, and scale,
  through `toron-bench`'s parent path, into a fresh output directory;
- seven rounds of 30 blocks (5 cells × 2 engines × 3 scales), 210
  blocks in all.

The order is declared before launch. Within round r:
- **cell order** is q01, q05, q09, q18, q21, rotated left by r − 1;
- **scale order** cycles by round: (8, 16, 32) in rounds 1, 4, 7;
  (16, 32, 8) in rounds 2 and 5; (32, 8, 16) in rounds 3 and 6;
- **engine order** is Toron then DuckDB in rounds 1, 2, 5, 6, and DuckDB
  then Toron in rounds 3, 4, 7.

Each scale therefore leads in at least two rounds.

*Enforcement.* Each block keeps ADR 617's controls:
- a 20-minute timeout that reaps the process group;
- a no-leftover-child check, and an empty-scratch check that fails when
  the scratch cannot be listed;
- the campaign stops on any isolation failure.

The unit's runtime limit is 6 hours. A block that times out is recorded
as incomplete, not as a runtime.

**The unit's limit reaps everything.** It runs with
`KillMode=control-group` and `SendSIGKILL=yes`, so a unit timeout kills
the whole control group, including the current block's descendants.
The `ExecStopPost` completion hook writes a durable marker and sends the
datagram whatever the result, so a timeout leaves a record naming it.

After a unit timeout, no diagnostic starts until no query child remains
and the spill scratch is verified empty. A truncated campaign cannot
pass the 210-block validator. Its completed cells may be reported only
as explicitly partial evidence.

*The untimed diagnostics.* ADR 617's diagnostics script, extended to
three scales, with its three kinds never mixed:
1. a stage-attributed Toron run per cell and scale, with node
   attribution, the join-build profile, and the scan ledger, and no
   receipt;
2. the q05 cap-check receipt, as its own run per scale;
3. DuckDB: the harness's profile sweep, then the direct scan-work
   profile.

Then the harness's `correctness` command, per cell and scale.

*The read-out.* ADR 617's **v3** readers, extended to three scales with
no loosening:
- campaign-structure failures are fatal, and the declared order is
  checked against the schedule above, regenerated in the reader;
- per-row and per-group validation as in v3, with the dataset
  fingerprint of each scale;
- correctness is checked first, with suppression;
- node-tree validity, and strict and top-level completeness named
  separately;
- stage ratios only between corresponding topologies;
- ADR 605's fold, reconciled with the ledger receipt;
- the DuckDB profiles re-verified from their preserved files.

Negative controls are run on the readers before their outputs are used.

*The report.* ADR 617's, per cell and engine, over three points:
- medians and ranges;
- SF16/SF8 and SF32/SF16 ratios;
- each engine's time per visited row, with its own counter;
- the gap at each scale;
- spill and RSS with their labels;
- per-stage ratios where the topology corresponds, with any topology
  change shown separately;
- diagnostic-minus-timed context.

Readings are candidates, or unresolved.

*Traces, alongside and read-only.* While this plan is reviewed, two
questions are traced in the code and the existing telemetry, without
running anything:
- what decides q21's deferral;
- which of q18's joins can spill, and what records it.

A code trace identifies a possible owner or condition, not which branch
ran. Runtime evidence is kept distinct from it.

If the traces find existing armed telemetry that records these
decisions, each variable is **named and reviewed before its diagnostic
launch**, including what it actually does:
- a receipt or validator that re-executes work goes in a **separate**
  diagnostic run;
- so does instrumentation that could change execution or admission;
- only a passive recorder may join the attribution run used for stage
  ratios.

If no safe passive signal exists, the trace stays read-only, and an
instrument is its own record.


## Acceptance

- The SF32 dataset on strix, with every file identical to bigfoot's.
- The executable from the plan commit, staged and hash-verified.
- The declared 210 blocks, every row retained, and the v3 readers
  passing, or named failures.
- The diagnostics, identity checks, and report above, for five cells
  at three scales on both engines. Or, for any part not established,
  the named reason.
- Plan commit, then one implementation commit with the results and the
  closure.


## Guardrails

- A diagnostic: no token, no cut, no saving projected.
- SF16 and SF32 timings never enter the sealed SF8 board.
- No ADR 588 or 597 verdict or variability bar across scales.
- Armed runs and profiles are never read as timed latencies.
- Cross-scale digests are not an identity gate.
- No SSH session or file write on strix between launch and the
  campaign's completion datagram.


## Risks

- **q21 at SF32** may spill several GB. If a block times out or is
  refused, that is a reported result, not a failure of the record.
- **q05's peak RSS** may approach 26 GB. That is within strix's memory,
  but it is reported.
- **The host budget is checked at launch.** Free disk and memory are
  observed on strix again immediately before launch. The extrapolations
  above are for planning only.
- **Page cache.** The three datasets total about 22 GB. Each block's
  own warm-up is the cache control, as before, and it is best-effort.
