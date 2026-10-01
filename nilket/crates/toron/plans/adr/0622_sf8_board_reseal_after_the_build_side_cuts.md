# ADR 622 - SF8 board re-seal after the build-side cuts

**Status:** PROPOSED. Successor to ADR 621. Main has changed since ADR
613 sealed the current-tree board (Toron 61.387 s against DuckDB
15.579 s, 3.940×). The source-changing commits since then:

| ADR | change | status |
|---|---|---|
| 615 | q05 replay-1 build refinements (instrument) | its sweep showed digest, fingerprint, route/topology and reconciliation-status parity apart from the added refinements; that does not itself rule out a timing effect |
| 616 | reused build batches skip the single-table exchange (`REUSE_BUILD_EXCHANGE = Bypass`) | retained: q05 −1,340.7 ms paired; q09 breadth +260.0 ms gain, no regression |
| 619, 620 | test-only benchmarks | no production change |
| 621 | inline fixed-width generic build keys (`GENERIC_BUILD_KEY_STORAGE = Inline`) | retained: q09 −1,053.4 ms paired; q01 no-change control |

ADRs 614, 617 and 618 changed no source.

Only q05 and q09 have paired gains since ADR 613. Other cells carry the
changes unmeasured:
- ADR 616's armed runs showed `prep_exchange` gone from q07 and q08 as
  well as q05 and q09;
- ADR 621's gate named q05 and q20 as changed but unpaired, and six
  cells (q01, q04, q06, q14, q19, q21) with unknown profile coverage.

This record re-seals the full 22-cell board on main's current tree, at
the anchor coordinate. It establishes:
- the campaign's current total and ratio;
- a current-tree baseline, with a sealed manifest, for the next paired
  records;
- the gap ranking by absolute time, from which the next owner is
  chosen.

It is a measurement, not a cut. It makes no retain or revert decision,
and it attributes no cross-board change.


## Decision

The procedure is ADR 613's (itself ADR 608's), verbatim, except for this
record's paths.

*The measured tree.* Main at this plan's commit. Its code equals
`artifact/adr621-treatment` (`f0b63449`); the trees differ only in
`plans/`. The ladder (Toron and toron-tpch, `--no-fail-fast`) runs on
that tree on bigfoot, with fmt checked. Then one release executable is
built with DuckDB `c478c0f4…`, recorded in
`target/tpch/evidence/build/adr622-measured.manifest`, and staged and
hash-verified on strix. Whether it reproduces ADR 621 treatment's bytes
(`18df5747…`) is reported; either way, the manifest built here is the
one the board binds. Strix's checkout is moved to this plan's commit
before the launch, so the seal tags the measured tree.

*The run.* One step, `perf --engines toron,duckdb --query all --runs 5
--warmup 1`, at the ADR 603, 608 and 613 coordinate:
- SF8 strict parquet;
- task memory and spill reserve 2 GiB each;
- partition parallelism 1, DuckDB threads 1;
- affinity `0-15`;
- DuckDB's default memory coordinate: `memory_limit` present and null,
  with provenance `default`. The observed read-back is reported.

**Required identity** on every row and in the header: `full_hash`
dataset verification, the unchanged SF8 dataset fingerprint, and the
measured executable. The gate's comparability and refusal rules remain
the authority.

**Runner safeguards.** Inheriting ADR 613's procedure does not mean
inheriting old runner defects. The current fail-closed safeguards, as
agreed after ADR 618, apply:
- the join-spill scratch directory is created before the first
  snapshot, and a failed listing stops the step;
- leftover-process checks;
- fresh output and host-record directories, never overwritten (an
  existing one refuses the launch);
- bounded unit and process-group cleanup (`KillMode=control-group`,
  `RuntimeMaxSec`);
- a launch wrapper that clears every inherited `TORON_*` variable, then
  exports and records only the coordinate. Diagnostic, receipt,
  ablation and skip-hash variables must be absent from the timed
  child's environment.

All scripts are staged before the launch.

It uses ADR 595's separated launch, with the read-out's after-snapshot
taken only after the completion datagram. Host scripts (`launch.sh`,
`readout.sh`, `snapshot.sh`) are generated from ADR 613's, with this
record's paths (`adr622-board`, `adr622-strix-host-record`,
`adr622-join-spill`, `adr622-measured.manifest`). They are
grep-checked for other ADR numbers, and staged before the launch. No
SSH to strix between the launch and the completion datagram.

*Sealing.* On `candidate_authority` true: `seal-evidence` with
`--remote seal`. The evidence tag is fetched to the workstation and
pushed with the closing commit. On a refusal, the artifact and its
reasons are retained unsealed, and no new current-tree baseline is
published. There is one attempt.

*The report*, as in ADR 613. Every figure is read from the artifact:
- **Per cell:** Toron and DuckDB medians, the ratio, the gap, and each
  engine's change from ADR 613 (and Toron's from ADR 603).
- **Digests:** each Toron digest compared with ADR 613's.
- **Totals:** the ratio, and the target arithmetic named twice:
  - the distance to 2× this board's DuckDB total;
  - separately, the distance to the fixed 31.366 s threshold.
- **Ranking:** the cells ranked by absolute gap (Toron − DuckDB ms),
  beside ADR 613's ranking. The ranking also lists each cell's Toron
  median, since absolute Toron time is what the total sums.
- **Stability:** per-cell stability under the gate.
- **Peak RSS and spill:** peak RSS with its parent high-water and its
  `above_parent_high_water` or `ambiguous` label (an ambiguous reading
  is not stated as a peak), and spill per Toron cell.
- **DuckDB's change:** its per-cell change, reported as observed
  cross-board change for context only. It is not an identified
  measurement of host drift.
- **Provenance:** the artifact, sealed manifest and reader hashes, the
  host-record paths and the attempt token.


## Acceptance

- The ladder green on the measured tree. The manifest staged and
  verified.
- The artifact, with `candidate_authority` and its reasons. If sealed:
  the sealed manifest's hash, the evidence tag, and the archived
  executables.
- The report above.
- Plan commit, then one implementation commit with the results and the
  closure. No code change.


## Guardrails

- **No attribution.** Per-cell changes from ADR 613 are not credited to
  ADR 616 or ADR 621, or apportioned between them. The paired records
  (q05 and q09 for ADR 616; q09 and the q01 control for ADR 621) are
  the only token evidence. Cells that changed without a pair, including
  q07, q08 and q20, are reported as observed, with no causal claim.
- A cross-board difference is not a paired gain. The q05 and q09 board
  changes are reported beside, not as, their paired figures.
- ADR 603 remains the historical anchor. This board becomes the
  current-tree baseline if sealed.
- One attempt, no relaunch.


## Risks

- **Cells that got slower.** A cell carrying an unmeasured path may be
  slower than on ADR 613. The board reports it as observed; only a
  paired record can attribute it.
- **Cross-board change.** The DuckDB column shows observed cross-board
  change as context. It does not identify host drift.


## Forward

- The next owner is chosen by absolute time on this board.
- Already queued: q18's stage breakdown and the targeted SF64 q05
  probe. Any cell observed slower here is a candidate for a paired
  record.
