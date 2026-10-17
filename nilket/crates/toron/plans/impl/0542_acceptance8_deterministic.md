# ADR 0542 acceptance 8: deterministic fixture plan

Status: selected plan; acceptance remains OPEN until paired executed evidence and gates pass.

Base: published C-S3b c059c3fb8e48443c2699fde0751257c10c1d25eb.
Historical parent P: cb7fdaf4d3d0e9c75dbc366b2d704a93011d5af8.
Historical implementation I: 0987e0f59f42cc0b40963e31d676bd4b1889b53c.
The landed fixture also runs on the current implementation descendant.

## Evidence and scope

The prior soak attempts remain unchanged: ac1cda18 had a parser defect,
6e04f0f5 missed the event floor, and 6df8c9a4 met the floor with no failure
signature. None discharges acceptance 8. The deterministic trigger replaces
hoping that a soak reaches an unbounded server write; it does not assert that
the old row boundary's cause has been identified.

ADR 0539 supports a separate compaction/reference-integrity hypothesis.
Ordinary row records are indexed directly during slab recovery; F-CTR-6's
counter tentative/marker split is not the same mechanism. Rotation and
compaction retirement therefore remain a separate paired boundary fixture.
Any failure at P is reported as a new finding, not relabeled as acceptance 8.

## Shared test seam

Use the existing AsyncFileOps interface and RecordingFileOps-style gate.
Add only a feature-gated SlabManager constructor accepting caller-supplied
AsyncFileOps, reachable from Toron through Wendelon's non-default test-support
feature. Apply exactly the same seam patch to P and I. Builds without that
feature retain their production construction path. Reuse existing guards for
test-support seams where applicable. A production-visible change beyond this
constructor is a STOP requiring review.

The injection holds fdatasync after append and before durability acknowledgement.
Record gate entry before sampling outcomes; do not infer commit knowledge from
the presence of appended bytes. Release for teardown is recorded separately
from the observation. Detached work is not claimed cleaned up merely because a
caller returned or the harness cutoff elapsed.

## Paired observations

1. No injection: exact batch count and successful reads on P and I.
2. Persistent hold at P: the batch has no response at the explicit harness
   cutoff, no typed cause or commit-knowledge result, and no exhausted retry
   budget. Record cutoff/ambiguous, never pass or silent success.
3. Transient hold at I, released before expiry: committed batch with exact count.
4. Persistent hold at I: typed deadline or no-progress cause within the
   predeclared deadline plus epsilon, with the independently justified
   commit-knowledge class for post-dispatch/pre-fsync injection.
5. Force slab rotation and a compaction retirement during INSERT batches while
   concurrent COUNT(*) reads run. Execute on both P and I. At I require exact
   final counts and consistent reads; at P retain the observed outcome,
   reporting any failure separately.

Before execution, freeze fixture inputs, gate-entry proof, clock bounds,
cutoff/epsilon, exact expected typed fields, boundary-event receipts and teardown
semantics. The seam and shared inputs are byte-identical across the pair;
revision-specific outcome assertions are explicit.

## Execution and publication

Use isolated worktrees and fresh dedicated target directories for P and I.
Retain successful compiler command/artifact provenance, Compiling lines,
executable SHA, actual --list names, raw logs and source-before/after hashes.
Never reuse a base executable as the subject. The P failing observation is
external evidence, not a failing test landed on main.

Sequence: selected census and review -> this plan commit -> test source ->
pre-seal check/format/guards -> unsigned review -> signed SOURCE -> standard
gates selected for the actual changed files -> FINAL and joint FF. No full
ladder is presumed necessary for a test-file-only change; any seam/build-input
change is disclosed for gate selection. No runtime deployment or soak is granted.

Commit subjects:
- plan: adr 0542 deterministic acceptance 8
- test: adr 0542 deterministic acceptance 8

After executed acceptance and publication, update the ADR status in the later
status-closure pass. Until then item 8 remains unresolved. F-CTR-5a follows this
fixture in the selected work order.
