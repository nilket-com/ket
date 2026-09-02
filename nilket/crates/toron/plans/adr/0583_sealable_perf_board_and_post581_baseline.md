# ADR 583 - Sealable perf board and the post-581 baseline

**Status:** OPEN. Decided; no implementation has landed. Re-enters the TPC-H perf
arc; closes ADR 581's last acceptance item when its own acceptance holds.


## Context

The last cross-engine number is the post-521 three-engine board: q01 through q22
at strict SF=8, Toron one task against DuckDB 1.5.4 pinned to one thread, a
median per-cell ratio of 4.26x and a summed-median ratio of 4.24x, 80.6 s against
19.0 s. That is SF=8, not SF=10; no SF=10 board of that kind exists, and no SF=10
fixture is present.

Several Toron arcs landed after that board. The two relevant measured
optimization arcs, ADR 554 and ADR 580, were measured on Toron's own timing
atlas, on the distributed arm, with no DuckDB arm, and 554 states its reductions
as non-additive; no later work produced a cross-engine board. So the ratio has
not been re-derived, and a projection from those wins is not a measurement. The engine's perf path is
unchanged since 580's retained cut; every commit since touches Wendelon, which
the in-process gauge never exercises, or the harness's identity plumbing.

ADR 581 made evidence content-addressed, but only the timing atlas and the
placement A/B emit run manifests. The ordinary `perf` board, which is the
command that produced the 4.26x table, writes its summary with no identity
header and no manifest, so it cannot be sealed and cannot carry authority.

Two controls the old board depended on were documented but not serialized into
its rows. The DuckDB thread pin was an environment setting the rows recorded as
null; the board itself named that as a harness gap. The DuckDB executable was
identified by version only, and it was invoked from a mutable path on every
run, which is the pre-581 problem in a second engine.

The old board's local artifact directory is gone and its pre-rebase build commit
no longer resolves, so its committed table is the only comparison source.


## Decision

**The perf board becomes sealable.** `perf` writes its canonical summary artifact
with the same `evidence_identity` header the atlas carries, and emits an unsealed
run manifest beside it.

**Candidate authority requires full-board integrity, not coordinate fields
alone.** A board is a candidate only when every one of these holds, each checked
by the gate and each refusal named:

- exactly q01 through q22, each run on Toron and on DuckDB, in the recorded
  query and engine order;
- 5 measured samples plus 1 warmup per cell, every cell passing and within the
  existing stability gate;
- release profile, and the dataset verified under full-hash mode;
- the dataset manifest fingerprint, recorded in the header and agreeing across
  every row, so the coordinate names which Parquet bytes ran and not only a
  scale factor and a mode;
- explicit 2 GiB task memory and 2 GiB spill budgets;
- Toron pinned to the historical one-task coordinate: ADR 527 added partition
  concurrency after the 521 board, so merely recording the current setting could
  admit a faster parallel coordinate as comparable. `TORON_PARTITION_PARALLELISM=1`
  is required, its provenance recorded, and every observed partition-work
  concurrency must be at or below one;
- DuckDB 1.5.4 at one thread, by staged executable hash;
- diagnostics disabled;
- deployment quiescent and the join-spill scratch empty before and after.

Without every item a partial or failed board could still seal with final
authority, which is the sealed lie the whole apparatus exists to prevent.

**Identity and comparability are different gates, and the two executables are
treated asymmetrically.** Within one board every row must share the staged
harness identity. Across canonical boards the Toron source tree and harness
hash are EXPECTED to change - that is what is being measured - and are reported
side by side rather than equated. The DuckDB staged hash is the control and MUST
match between canonical boards; a board whose DuckDB hash differs from the
baseline's is comparable only as a new reference, never as the same one. The
comparison against the 521 table is explicitly legacy and conditional, because
that board's DuckDB hash is unavailable and only its version is known.

**DuckDB is staged, not merely hashed.** Today the harness resolves the DuckDB
executable from the environment and invokes that mutable path on every run, so a
hash taken before timing proves nothing about what ran. `toron-bench` accepts a
DuckDB executable, stages it read-only under its hash, verifies it, hands the
staged path to the harness, and `seal-evidence` archives it with the other two.
That is a manifest schema change, so the build manifest version is bumped rather
than v1 redefined. A DuckDB arm is refused unless the one-thread pin is set,
with the reason named. Every DuckDB row and the artifact header record the
thread setting, the version, and the staged executable's hash. The version
field already exists; staging, the hash, and the refusal are new.

**The baseline is the same Toron-and-DuckDB coordinate as the 521 board, run
once.** It is not the three-engine board unless Polars is actually included.
Build through `toron-bench build` with the release profile; the wrapper invokes
Cargo directly, so the cgroup script's backend setting has no meaning here and
is not part of the coordinate. Clear the join-spill scratch and use a fresh
output directory. Run q01 through q22, Toron and DuckDB, strict SF=8 persisted
Parquet, 2 GiB task memory and 2 GiB spill reserve, 5 measured runs and 1 warmup
per engine per cell, DuckDB 1.5.4 at one thread from the staged executable.
Seal through `seal-evidence`.

**Comparison is per cell against the committed 521 table**, reporting both
aggregates the old board reported, the median per-cell ratio and the
summed-median ratio, and the absolute Toron-minus-DuckDB gap per cell. The
comparison names itself as table-to-table: the old artifact cannot be reopened.
The sealed board becomes the canonical baseline; the next owner is selected by
absolute gap from it.

**No projection is published.** Until the sealed board exists, the ratio is
"unmeasured since 521", not a number.

**SF=10 is out of scope.** It is a separate coordinate with its own fixture and
its own reference sweep, and it follows once the arc has an owner.


## Acceptance

- A `perf` artifact header carries the content identity, the coordinate fields,
  and the dataset manifest fingerprint, and a run launched outside
  `toron-bench run` has candidate authority false with
  `content_identity_manifest_absent` among its reasons.
- A board missing one cell, one sample, or one engine, or with one failing or
  unstable cell, is not a candidate, with the missing or failing item named;
  the discriminating fixture drops exactly one DuckDB sample from one cell.
- Two `perf` artifacts that differ in any coordinate item, including the dataset
  fingerprint and the parallelism pin, are refused as incomparable with the
  item named; two that differ only in Toron identities are comparable and the
  report shows both identities; two that differ in DuckDB staged hash are
  refused as the same reference, with both hashes named.
- A board run with the parallelism pin unset, or set to 2, is refused authority
  with the pin named, and so is one whose observed partition-work concurrency
  exceeds one under a pin of one.
- A DuckDB arm launched without the one-thread pin, or from a path that is not
  the staged executable, is refused before any query runs, with the reason
  named; with both, every DuckDB row carries the thread setting, the version,
  and the staged hash, and the sealed manifest archives the DuckDB executable.
- Both manifest schemas change: the staged DuckDB identity extends the build
  manifest and its archived location extends the run manifest. Both versions are
  bumped; a v1 build manifest and a v1 run manifest still decode under tests
  that pin their bytes; an unknown version of either is refused.
- The baseline run seals: archived executables, a tree tag, and a sealed
  manifest with final authority true.
- The results document cites the source tree id and the executable hashes, not a
  commit; and the arc's history contains no stamp-only commit. That discharges
  ADR 581's final acceptance item.
- The results document reports both aggregates and the per-cell table against
  the 521 numbers, and states the comparison as table-to-table.


## Guardrails

- The gate compares identities and coordinates; it computes neither. Hashing
  and pinning happen before timing.
- The local deployment stays offline. The board is in-process and touches no
  Wendelon node.
- Sealing is manual, as ADR 581 fixed. The timed run never mutates git.
- One implementation commit. ADR 581 exists so the uncommitted harness change
  can be reviewed, built, and measured as a tree. The evidence tag names that
  reviewed pre-results measurement tree; the results and the closure text are
  then added, so the final commit's tree is that implementation plus its
  results and is not itself the measured tree. There is no harness commit
  followed by a results commit, and nothing else lands under this number.


## Risks

- Table-to-table comparison inherits the old board's documented controls that
  were not serialized into its rows. If the
  new board disagrees with the old one in a direction the engine cuts cannot
  explain, the disagreement is reported as such, not resolved by choosing the
  friendlier reading.
- The DuckDB executable is an installed binary outside the source closure.
  Staging and archiving make its bytes recoverable and comparable between
  boards; what stays unavailable is its source and build provenance.
- Run-to-run drift on this host has been up to 10 percent on the atlas. A single
  board reports drift as a caveat, not as a change.


## Forward

The sealed board names the next owner by absolute gap. The standing candidates
are the multi-batch probe residual ADR 580 left unattributed and q21 if it still
leads. SF=10 follows as its own coordinate with its own fixture and reference.
