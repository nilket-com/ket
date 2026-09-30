# ADR 608 - SF8 board re-seal on the current tree

**Status:** CLOSED, SEALED. One board on strix (results in
`plans/impl/0608_sf8_board_reseal_on_the_current_tree_results.md`;
artifact `7662f6f2…`, evidence tag
`evidence/6c052239b854-7662f6f22943`). Toron 62.706 s against DuckDB
15.600 s, 4.020×. Toron is 31.506 s above 2× this board's DuckDB total,
and 31.340 s above the fixed 31.366 s threshold. Every digest equals
ADR 603's, and every cell is stable. No cross-board change is
attributed. Successor to ADR 607. Since the sealed ADR 603
anchor (Toron 63.430 s against DuckDB 15.650 s, 4.053×), main has
retained one global change: ADR 606's `WRAPPED_CAP_CHECK_BATCHES =
Reuse`. It was measured by pairs on q05 (retained, 444.7 ms) and q09
(not established, 72.3 ms), and on no other cell. This record re-seals
the full 22-cell board on main's current tree at the anchor's
coordinate. It establishes:
- the campaign's current total and ratio;
- a current-tree baseline, with a sealed manifest, for the next paired
  records. The first is the grouped-fold work, beginning with q01 and a
  breadth cell.

It is a measurement, not a cut. It makes no retain or revert decision,
and it attributes no cross-board change to any token.


## Context

**Why now.** ADR 607's Forward named this step. A global token's effect
across 22 cells is unmeasured. The campaign's total cannot be stated
from two cell pairs.

**What comes after.** A grouped-fold direction, agreed with Codex, from
ADR 604's q01 attribution. q01 is join-free and routes `scan_grouped`.
Its fold takes 3,574 ms of 4,486 ms, of which `row_lookup_update` is
2,643 ms. The two-UTF-8-key path builds owned `String` group values per
row. By a code-path estimate (2 keys × ~48 M rows, not an
allocation-counter reading), that is about 96 M allocations. ADR 609
plans a borrowed-key lookup as one token, against this record's q01
baseline. A batch group-id and columnar-accumulator change follows as
its own record, only after ADR 609 closes. The fold's time beside
DuckDB's `HASH_GROUP_BY` (433 ms) is diagnostic, not a comparable rate.


## Decision

*The measured tree.* Main at this plan's commit. Its code equals
`artifact/adr606-treatment` (`85ff3958`). The trees differ only in
`plans/`. One release executable is built on bigfoot under recipe
`9af51cbc…` with DuckDB `c478c0f4…` (v1.5.4), recorded in
`target/tpch/evidence/build/adr608-measured.manifest`, then staged and
hash-verified on strix. The ladder (Toron and toron-tpch) runs on the
same tree on bigfoot first. The closing commit differs from the
measured tree in the plan documents only.

*The run.* ADR 603's anchor step, unchanged:
- one step, `perf --engines toron,duckdb --query all --runs 5 --warmup 1`;
- the coordinate: SF8 strict parquet, task memory and spill reserve
  2 GiB each, partition parallelism 1, DuckDB threads 1, affinity
  `0-15`, and no DuckDB memory limit (provenance `default`);
- ADR 595's separated launch: one SSH session runs `launch.sh`, which
  takes the before-snapshot and schedules a transient unit 120 s out;
- nothing else runs on strix until the completion datagram arrives.
  Then a read-out session takes the after-snapshot.

The output directory and the join-spill scratch are this record's own,
fresh and empty. The host scripts are generated from this record's
paths and grep-checked for other ADR numbers before launch.

*Sealing.* On `candidate_authority` true, `seal-evidence` runs with
`--repo ~/work/ket`, `--archive-root target/tpch/evidence/archive`, and
`--remote seal`, as ADR 603's was. The evidence tag is fetched to the
workstation and pushed with the closing commit. On a refusal, the
artifact and its reasons are retained, nothing is sealed, and the
record closes on the refusal. There is one attempt.

*The report.*
- Per cell: Toron and DuckDB medians, the ratio, the gap, and each
  engine's change from the ADR 603 anchor.
- The 22-cell totals and the ratio, with the target arithmetic named
  twice:
  - the distance from Toron's total to 2× this board's DuckDB total;
  - separately, the distance to the campaign's fixed original
    threshold, 31.366 s (2× ADR 591's DuckDB sum), which ADR 603 used.
  No new absolute target is substituted.
- The gap ranking.
- Per-cell stability under the gate's `(max − min) / median ≤ 0.20`.
- Spill per Toron cell. Peak RSS per cell with its provenance, as ADR
  603 reported it: the parent high-water at spawn, and the
  `above_parent_high_water` or `ambiguous` label. An `ambiguous`
  reading is not stated as that cell's peak.
- DuckDB's per-cell change from the anchor, reported as drift for
  context. Its executable and settings are unchanged. DuckDB's movement
  is not a numerical bound on Toron's change. The record declines to
  apportion any cross-board Toron change without a pair, independently
  of DuckDB's movement.


## Acceptance

- The ladder green on bigfoot on the measured tree. The manifest, with
  the executable's hash, staged and verified on strix.
- The board's artifact, with `candidate_authority` and its reasons.
  If sealed: the sealed manifest's hash, the evidence tag, and the
  archived executables.
- The report above, and the gap ranking against ADR 603's.
- Plan commit, then one implementation commit with the results and the
  closure. No code change is expected. If the harness needs one, it is
  stated and reviewed before the build.


## Guardrails

- No attribution. A per-cell change between this board and ADR 603's
  is not credited to ADR 606 or to any other change. Only paired
  records attribute. Cells other than q05 and q09 carry the token
  unmeasured, and the board reports them without claims.
- The ADR 603 anchor remains the historical anchor. This board is the
  current-tree baseline. Later pairs bind whichever the plan names.
- One attempt. No relaunch under this record.


## Risks

- Host drift between the boards. The DuckDB column shows it as
  context. The record states it and does not correct for it.
- A cell may fail the stability gate or the authority checks. Then the
  artifact is retained unsealed, with its reasons, and the next
  record's baseline is decided in its own plan.
- The retained token raised peak RSS on q05 (+1.14 GiB) and on q09
  (about +123 MB). Other cells' RSS changes are observed here for the
  first time, without a paired control.


## Forward

- ADR 609, conditional on this board sealing: the q01 borrowed
  multi-key group lookup, one token, against
  this board's q01 cell. It needs explicit hash and equality semantics
  for nulls, numeric and float canonicalization, and UTF-8 bytes;
  mechanism evidence for allocation and lookup; digest parity; RSS; and
  a pair. A breadth cell is added if its current plan and receipt
  confirm the same multi-key UTF-8 grouped-fold owner (q18 first
  checked). Otherwise another eligible grouped cell is chosen from this
  board.
- The batch group-id and columnar-accumulator change is a separate
  record, after ADR 609 closes and informed by its measured residual.
