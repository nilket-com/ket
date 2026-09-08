# PRODUCT-0161 implementation evidence

Status: implemented 2026-09-07. The record remains proposed.

Review correction 2026-09-08: the Python harness used for the historical runs
below has been removed. `uat/agent_notebook_drive.sh` now uses Bash/jq and a
90-second GNU timeout (with a five-second kill grace) for every `act`. The same
19-action real-window journey passed, including an output directory containing
spaces; receipts are in `/tmp/polariton-bash-qHHllq/receipt with spaces`.
A synthetic unknown answer stopped after exactly one invocation, with no replay.
Source kinds now serialize as `source.parquet` and `source.csv`, pinned by a
serialization/deserialization test. Earlier collected rows retain their original
spelling; no existing telemetry is rewritten.

A fresh empty start opens and executes a small Markdown notebook on scratch.
It explains the example, offers CSV/Parquet opening and a kept own-data figure,
and offers the optional Toron workspace separately. First-run startup suppresses
workspace discovery. The marker affects only later empty starts.

The missing automation was notebook authoring, delivered first in PRODUCT-0162.
The welcome button now uses that same open/edit/run owner: an asynchronous file
open cannot silently overwrite a human edit because replacement validates the
captured document generation. Its guided sequence runs the SQL and plot cells.
The optional Rune showcase opens a new pane, preserving the notebook. A focused
Rune source now keeps its own Run action instead of borrowing an unrelated
notebook's execution context.

A kept-analysis report requires an opened own file, the data cell's successful
run, an own-file plot, a deliberate subsequent source/visibility change, and a
successful shared publish job. It is an observation of this guided journey,
not a general proof of SQL data lineage. Reporting emits closed step/reason
codes, versions, identifiers and times; paths, query text and data stay local.

## Reporting and persistence

Reporting is off by default and requires an explicit endpoint and PKI directory.
There is no discovery fallback. Submission only tries a bounded channel; a
background writer persists immutable generation-tagged segments. Writers seal
synced batches, and drainers read only sealed segments under the stable lock.
Progress binds its acknowledged prefix to the segment and generation with a
checksum. Missing or invalid progress replays; a retry carries identical values
under the same NOT NULL primary key. Unknown delivery stops that drain.

Implementation refined three clauses in the plan. Batches seal immediately
rather than leaving already-synced batches in an active file. A failed write
therefore discards its whole unsynced batch and stops persistence for that run.
Segment order uses an ordinal allocated under the lock, independent of wall-clock
rollback. Disable publishes and syncs the revoked generation before attempting
its final settings report, so a crash cannot make the next launch repeat it.
A crash after revocation may lose that final observation; it cannot resurrect
old reports. Directory entries are synced as part of durable publication.

## Exercised journeys

The latest debug binary completed all 19 acknowledged actions through
`uat/agent_notebook_drive.py` in live window
`2ecab2f436b77a536ffca482578972f1`. It ran the example, opened the repository's
`nilket/crates/toron-tpch/unused/strict/sf0_01/nation.parquet` fixture, edited and
ran its SQL/plot cells, changed the y expression after rendering, and published.
The file exposed three columns, two numeric. Commands, final wire answers and
package are retained under `/tmp/polariton-0161-live/journey-last`. No pointer or
keyboard injection was used. The window was closed after verification.

An earlier complete 19-action run with the destination stopped retained seven
reports locally and completed its publication. Starting the collector delivered
those reports with their original ids and timestamps. Another run induced an
unsupported-file failure: the local refusal named the path and the destination
received only `file.unsupported`. A fresh reporting-off run completed the same
journey, produced no reporting directory, and its process network trace contained
no AF_INET calls; ordinary display/agent AF_UNIX access remained. Evidence is in
`journey-final`, `journey-verified`, `journey-off`, and `off-network.log` below
that same temporary directory.

Starting an off-configured window against previously enabled state left exactly
`reporting.lock` and the disabled `generation.json`; the old installation id and
destination were cleared. The destination held one final `reporting.disabled`
report. This is an observed settings transition, not a population denominator.

The optional-workspace integration test was explicitly run in two isolated
profiles. Against the task-owned ready workspace, the actual welcome action
seeded and ran the Rune source while preserving the notebook and produced a
second plot. With no launcher on PATH, it produced
`workspace.launcher_missing` and retained the notebook plot. This did not repeat
fresh daemon provisioning: the user's existing storage ports remained occupied
and untouched. PRODUCT-0006's launcher tests run in the ordinary suite. The
virtual Rune pane has no file-backed view attachment; the test asserts the
rendered plot rather than inventing a view name.

## Business view

Reports were delivered over authenticated TLS to an explicitly configured
collector on port 17761, in task tenant 161 on the local storage node. A separate
task-owned loopback query head on 17762 exposed the same tenant through an
isolated local-workspace advertisement for Polariton. No ordinary user's local
workspace was silently made a reporting destination.

The checked-in refresh SQL uses intermediate tables within the current query
exchange limits. Potentially empty failures/elapsed outputs use explicit table
schemas plus INSERT SELECT rather than relying on empty CTAS type inference.
A reader kept open across table replacement returned query_invalid_program;
restarting that task-owned reader after refresh resolved it. This cut does not
claim cross-head schema-cache invalidation. Schedule refresh before opening a
fresh reader until that independent storage/query behavior is addressed.

The final driver published all five figures: funnel, reporting state, last
observed step, failure reasons and elapsed time. Digest:
`34beaa4b599f5a738935ffd2f74a2b640058283f9fd5ede7ec178e504c2f7e7a`.
Receipt and package: `/tmp/polariton-0161-live/business-final-receipt.json` and
`business-final-package`. At that snapshot, Run was observed in seven recorded
runs and unknown in one; own-file and kept-analysis steps were observed in three
and unknown in five. These rows come only from the operator's test processes.
They establish nothing about an external customer funnel or total run population.

## Verification and limits

Reporting tests cover unknown acknowledgements, identical retries, corrupt and
misbound progress, concurrent drainer exclusion, cap admission and partially
acknowledged eviction, dead-active recovery, live-active exclusion, and durable
revocation followed by re-enable. Child-process crash tests kill execution after
append, before progress rename, and after durable revocation. A separate child
uses an actual file-size limit to force a partial append failure and checks that
neither an active nor a sealed batch remains.

The application tests exercise scratch startup, the shared own-file path and
publish conditions, marker behavior, reporting configuration, and preservation
of the notebook when the optional workspace source opens. The architecture guard
pins closed reason serialization and forbids reporting destination discovery.

Final Polariton ladder: 1,279 binary tests passed, 26 evidence tests ignored;
129 architecture guards passed; reduced-head all-target check, workspace
all-target check, and formatting passed. Both explicitly selected workspace
integration scenarios also passed. No TPC-H performance run was part of this cut.
The partial-write child isolates stdout/stderr: otherwise its file-size limit
also breaks the inherited ladder log, an issue caught during the full rerun.

Validation is on Linux. Mac and Windows acceptance remains outstanding. The
real-window receipts establish commands and projected figures, not a visual
polish audit. The workspace branch used an already-ready isolated query head;
fresh child provisioning remains a separate operator check. PRODUCT-0158's
saved-session acceptance is unchanged by this cut.
