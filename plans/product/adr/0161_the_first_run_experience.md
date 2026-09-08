# ADR 0161: the first-run experience

Status: proposed 2026-09-07, second revision under review. Application
lane, third record, and the first record of the sales and marketing lane.
Brief 0001 names the first customer journey - discover, get, run, first
useful analysis - and chooses its "Run" row as the first cut: a person opens
a fresh installation and, with nothing else present, sees a working example,
is told what they are looking at, and is handed the next step toward their
own data; together with the reporting for that step and the Polariton
business view over what it reports. This record decides what the first-run
shows and on what, how reporting is enabled and where it may go, the
complete delivery contract for reports, and what the business view may
claim. It adds no verb to PRODUCT-0158's surface and no query to any public
page.

## Context

### What a fresh installation is today

`polariton` with no file opens a window on an empty ephemeral buffer. A
SQLite scratch database is opened at startup so SQL always has a target. The
local workspace attaches only if `nilket local` is already running; when it
is absent, PRODUCT-0006 offers a "Start workspace" button that launches
`nilket local --data-dir <profile>` as a child of the window and attaches
when it is ready. There is no first-run state, no example, and no reporting
of any kind.

### Where Rune runs

Rune relations execute inside Toron: the desktop sends the source to the
workspace and receives artifacts. Nothing on the client lowers Rune to SQL,
and the Parquet and SQLite adapters speak SQL only. A Rune showcase therefore
needs a workspace. A Markdown notebook with SQL cells and plot cells needs
only the scratch database and already produces a figure through the same
kernels and the same publisher.

### What Toron does with a duplicate key

Grounded on the local deployment on 2026-09-07: on a table declared with a
`NOT NULL PRIMARY KEY`, a second `INSERT` of an existing key is acknowledged
`OK rows=1` and supersedes the row; a `SELECT` afterwards returns one row
carrying the newest values. There is no `ON CONFLICT` syntax and none is
needed: an insert retried with the same key and the same values is
acknowledged exactly as the first was and leaves one row.

### What the brief requires of this cut

First-run must succeed offline, with no workspace, no account, and no
network. Reporting must require explicit enablement and a configured
destination; an ordinary person's local workspace is never a destination.
Delivery failures must stay out of the customer's path. The business view
may claim only observed dogfood activity.

## Decision

### 1. First-run opens an example that needs nothing

On a start with no initial file and no first-run marker in the
application's state directory, the window opens a bundled notebook instead
of an empty buffer: a short explanation of what Polariton is, a SQL cell
over a small dataset the notebook seeds into the scratch database with
embedded statements, a plot cell that renders it, and a closing cell that
says what to do next. It runs with no workspace, no account, no network, and
no file on disk beyond the marker it writes. The marker is written when the
notebook has been shown once; opening a file or a session never shows it.

### 2. The Rune showcase is offered, not required

The closing cell explains that Rune views run in a workspace and points at
the "Start workspace" button PRODUCT-0006 already provides. When the person
starts one and it reports ready, first-run seeds the same dataset into it
and opens the Rune showcase over it. Declining, or a workspace that fails to
start, leaves the notebook example standing; the person has still seen a
working figure. Both paths are reported as distinct outcomes, and a
workspace that fails to start is a failure report with a reason code,
because that is exactly a place people stop.

### 3. The next step is the person's own file

The closing cell offers one action: open a Parquet or CSV file of theirs,
which connects the file adapter and opens a notebook with the same shape
over their columns. The guided path ends at a kept figure as the brief
defines it: published or saved, with one deliberate change after the first
render. The steps of that path, and their refusals, are reported with
reason codes; the product's own diagnostic text - which can name a path, a
column, or a connection - stays in the local log and the status detail and
never leaves the machine.

### 4. Reporting is off until enabled, and enabling names a destination

Reporting defaults off. It is enabled only in `init.rn`, with a destination:
the endpoint of a Toron workspace and the PKI profile to reach it. Enabling
without a destination is refused at configuration load with a diagnostic,
so an enabled installation always knows where its reports go. The
operator's own machines enable it against the operator's workspace; nothing
in the product ever selects the person's local workspace, or any discovered
workspace, as a destination.

The first-run notice says that reporting is off, what would be reported if
it were on, and where the setting lives. Off means silent: nothing is
collected, nothing is queued, nothing is sent.

**The transition is startup-only in this cut, and the generation file is
its authority.** `init.rn` is read at startup, and there is no
in-application toggle, so the only way reporting turns off is a launch that
reads it off. That launch performs the transition on a background task,
never on the first-run path: it takes the reporting lock (decision 6),
which waits for any drain attempt in progress to finish or time out; sends
the one final "reporting disabled" report, once, on its own connection,
with a bound of two seconds, and does not wait beyond that; publishes a
new generation file that says *disabled* by the durable sequence of
decision 6, so that the revocation is on disk before anything else
happens; then deletes every segment and progress file; and releases the
lock. A crash between the revocation and the end of cleanup is recovered
at the next launch, which repeats the cleanup: every segment carries its
generation, and one whose generation is not the current one is deleted
wherever it is found. Re-enabling later mints a new `install_id` and a new
generation, so nothing written under a revoked generation can ever be
recovered or sent. Every writer and every drainer, in
any process, checks the generation file at each point where it would
mutate the queue directory - creating a segment, sealing one, draining one,
deleting one - and on a mismatch stops for good: a writer discards its
channel and removes its own active segment, a drainer submits nothing more.
Nothing a running process holds can be recalled by another process, and
the record does not pretend otherwise: the bound is that an older window
may complete the drain attempt it was in the middle of, and no more, and
that an attempt already submitted with an unknown outcome may still
execute. If the final report does not land, nothing retries it.

### 5. What a report is

One row in the `journey_reports` table at the destination:

| column | meaning |
|---|---|
| `report_id` | random, unique per report; the idempotence key |
| `install_id` | random, minted when reporting is first enabled, reset when it is disabled |
| `run_id` | random, minted at each process start; a journey on the desktop is one run |
| `sequence` | strictly increasing within a run, assigned at submission; ordering within a run is by `sequence`, never by clock |
| `journey_id` | the token carried from a page, when one exists; else absent |
| `step` | `run.opened`, `first_run.shown`, `example.ran`, `workspace.offered`, `workspace.started`, `workspace.failed`, `own_file.opened`, `first_analysis.kept`, `reporting.enabled`, `reporting.disabled` |
| `outcome` | `ok` or `failed` |
| `reason` | a code from a bounded list defined in one place - for example `file.not_found`, `file.unsupported`, `column.not_plottable`, `workspace.launcher_missing`, `workspace.start_timeout`, `workspace.exited`, `example.seed_failed` - or, on a kept figure, the source kind; never free text |
| `dropped` | the count of reports dropped locally since the last report that carried one; zero otherwise |
| `platform` | operating system and architecture |
| `app_version` | the build |
| `at_ms` | when it happened, from the desktop's clock, for display and for time between steps; never for ordering |

Nothing about the person's data, queries, figures, paths, hostname, or
account is a column. The reason list is closed: a new code is a change to
this table, and a guard holds that no string from an error type reaches the
`reason` column.

### 6. The delivery contract

One protocol, in which every file has exactly one mutator.

- **Submission never blocks a step.** A step hands its report to a bounded
  in-memory channel of 256 and continues at once. If the channel is full
  the report is dropped and counted; the count rides the next report's
  `dropped` column. Nothing on a step's path takes a lock, opens a file,
  or touches the network.
- **Segments, one writer each.** Each run has one writer task and one
  **active segment** at a time, a file named by `run_id` and a segment
  number in the queue directory, which only that writer ever opens for
  writing. Its header names the run's pid and process start token and
  the **generation** it was written under. The writer appends a batch of up to 256 lines, calls
  `fdatasync`, and, when the segment holds 256 reports or 64 KiB or the
  run is quiescent for ten seconds, **seals** it: it takes the reporting
  lock briefly, re-checks the generation file, renames the file from its
  active name to its sealed name, syncs the directory, and releases the
  lock. A sealed segment is immutable and is the only kind a drainer ever
  reads. The rename is what publishes the sync boundary: sealed means every
  line in it was synced before the name changed, and no other process has
  to infer a boundary from line endings.
- **A failed append.** If an append or a sync fails, the writer truncates
  the active segment back to the length it last synced, syncs again, and
  seals what remains; if that fails too, the writer abandons the segment
  by removing it and stops persisting for the rest of the run, counting
  everything thereafter as dropped in memory. It never appends after a
  failure it has not recovered from, so a torn tail can only exist in an
  active segment of a run that died.
- **What a drain reads.** Under the lock, the drainer first rejects: any
  segment, sealed or active, whose header generation is not the current
  one is deleted with its progress file and never read further, whatever
  process wrote it and whether or not that process is alive. Then it
  reads every sealed segment of the current generation, oldest first. An
  active segment of the current generation whose run is dead - its pid
  gone, or its start token no longer matching the header - is recovered:
  complete lines are kept, a torn tail is discarded and counted as
  dropped, and the segment is admitted by the same rule the writer uses
  (the bound below) before it is sealed and joins the queue. An active
  segment whose run is alive is never touched. Drainers never rewrite a
  segment.
- **Progress, not compaction.** Beside each sealed segment the drainer
  keeps a progress file holding the count of leading reports acknowledged,
  republished after every acknowledgement by the durable sequence below.
  A segment is deleted, with its progress file, only when every report in
  it has been acknowledged. Resuming a drain starts at the count in the
  progress file, so the report at that index is attempted again. A
  progress file names the segment it belongs to, the generation, and the
  count, and carries a checksum over all three. A missing progress file
  means zero and replays the segment, which is safe because a repeated
  insert supersedes with identical values. A progress file that fails any
  check - unparseable, a count past the segment's length, a segment name
  or generation that is not this one's, or a checksum that does not match
  its contents - is treated as missing and replays from zero. That is
  what makes the claim hold: a corrupted count that stays in range still
  fails the checksum, so a bad file replays rather than skips.
- **Durable publication.** Every metadata file - a progress file, the
  generation file - is published the same way: written in full to a
  temporary name in the same directory, `fdatasync`ed, renamed over the
  final name, and then the directory is synced. A reader that finds the
  temporary name ignores it. Sealing is the same sequence applied to a
  segment. Nothing that depends on a publication proceeds until the
  directory sync has returned: cleanup after a revocation, deletion after
  a progress write, the next attempt after a progress write.
- **Bound, one admission rule.** Across sealed segments the queue holds
  at most 10,000 unacknowledged reports. Admitting a segment - a writer
  sealing its own, or a drainer sealing a recovered one - happens under
  the lock by one rule: evict the oldest sealed segments until the new
  segment fits, deleting each with its progress file, and count as dropped
  only the reports in each evicted segment that were not yet acknowledged.
  Both paths call the same rule, so recovery cannot exceed the bound.
- **The lock.** One stable file, `reporting.lock` in the queue directory,
  distinct from every segment, held under an exclusive advisory lock for
  every mutation of the directory by anyone but a segment's own writer:
  sealing, recovery, progress, deletion, the transition. Only the holder
  drains. `polariton report` and the application's drainer take the same
  lock, and the second refuses with a message naming the first. Multiple
  windows each append to their own active segment without the lock and
  contend only at seal time, which is brief and off every step's path.
- **Trigger.** The drainer runs on a timer while reporting is enabled and
  after every seal, and `polariton report` drains on demand. Neither runs
  unless reporting is enabled, a destination is configured, and the
  generation file agrees.
- **Delivery, one attempt at a time.** The drainer inserts one report at a
  time, in segment order, by `report_id`, over the Toron client on the
  configured PKI, and never has more than one attempt outstanding. A
  report is counted acknowledged in the progress file only when its insert
  is acknowledged, and an acknowledgement means what Toron's durability
  doctrine says: the row is durable at the destination.
- **Unknown outcomes stop the drain.** An attempt that times out or loses
  its connection has an unknown outcome: the insert may still execute at
  the destination. The drainer does not advance past a report whose
  attempt is unresolved; it ends the drain there, and the next drain
  begins by attempting that same `report_id` again. Attempts are counted
  per report and are unbounded; distinct report ids are what the table
  holds. Because a repeated insert of the same key is acknowledged and
  supersedes the row with identical values, any number of late executions
  of earlier attempts leave one row. The record guarantees that the
  destination never holds a row for a report that was neither queued nor
  the final disabled report, never holds more than one row per
  `report_id`, and that progress advances only on an acknowledged attempt.
  It does not guarantee that a report's earlier attempts have stopped
  executing; they may, and they change nothing.
- **Failure stays out of the path.** The writer's and the drainer's errors
  go to the log and to the status line's detail, never to a dialog, and
  never to the first-run notebook. A destination that is down is
  ordinary: the segments wait.
- **Two kinds of off.** A fresh installation that has never enabled
  reporting has no queue directory at all: no channel, no writer, no
  segment, no lock, no generation, no drainer. An installation that was
  enabled and then disabled keeps its queue directory with the lock file
  and a generation file that says *disabled*, and nothing else: no
  channel, no writer, no segment, no drainer. Gate 4 tests both.
### 7. The business view

A Rune source, checked in beside the showcases, over `journey_reports`,
published as a package by the PRODUCT-0158 driver from a commands file. It
is the brief's view - arrivals when a page exists, the funnel with observed,
absent, and unknown as separate series, stops by platform, failures by
reason code, time between steps, and observed reporting-off runs -
restricted in this cut to what desktop reports can show: the funnel starts
at `run.opened` because nothing upstream reports yet, and a journey is one
`(install_id, run_id)` ordered by `sequence`, so two launches on one
installation are two journeys and never merge.

The evidence for this record says, in as many words, that the rows are the
operator's own machines with reporting enabled, that they prove the
reporting path and the view, and that they establish nothing about an
external customer funnel.

### 8. What this record does not decide

The discovery page, the download path, the per-visit receipt, and the
shadow are their own records. A journey token carried from a page is
accepted if present and never minted here. Rune over the file adapters
without a workspace is a plotting-lane record if it is ever wanted. Whether
reporting should default on in a later cut is decided then, with the
observation contract unchanged. An in-application toggle for reporting is
not in this cut; the setting is `init.rn` and the transition above.

## Acceptance gates

1. **Nothing present.** A fresh state directory, no `init.rn`, no running
   workspace, and no network route: `polariton` opens the notebook example
   and the plot cell renders. A test proves that no workspace is discovered
   or launched, that no reporting channel, queue, or drainer exists, and
   that no connection is attempted to any endpoint; ordinary platform
   access - the display, fonts, system libraries, the application's own
   directories - is expected and not asserted against. The marker is
   written once, and an initial file suppresses the example.
2. **A kept figure from the person's file.** An operator's hand check: a
   fresh installation, the example, then a Parquet file of the operator's
   own through the closing cell's action, one deliberate change after the
   first render, and a published package. Recorded with the build and the
   file's shape, not its contents.
3. **The workspace path.** With `nilket` on the path, "Start workspace"
   from the closing cell seeds the dataset and opens the Rune showcase;
   with `nilket` absent, the failure is reported as
   `workspace.launcher_missing` and the notebook example still stands.
4. **Off is silent, in both of its kinds.** On a never-enabled
   installation, a full first-run and a kept figure create no queue
   directory and open no connection; on an installation disabled after
   cleanup, the same run leaves the lock and the disabled generation file
   untouched and creates nothing else; a test asserts the state
   directory's contents in both cases. Enabling without a destination is
   refused with a diagnostic at load.
5. **Reason codes only.** A guard asserts the `reason` column is written
   from the closed list and never from an error's text; a test induces a
   file refusal whose diagnostic names the path and finds the report
   carrying the code and the log carrying the path.
6. **The delivery contract, each clause.** Submission returns before any
   disk write and a full channel is counted in `dropped`; a drainer reads
   only sealed segments, and a writer killed between an append and its
   seal leaves an active segment that the next drainer recovers with its
   torn tail discarded and counted; a writer whose append fails truncates
   to its last synced length and seals, and never appends past an
   unrecovered failure; two launches write two segments with distinct
   `run_id`s and `sequence` orders within each; a drain against a local
   Toron advances progress only on acknowledgement and one attempt at a
   time; a drain whose attempt times out ends at that report, and the next
   drain begins with it; a drain killed after the insert and before the
   progress write leaves the report at the same index, and the retry is
   acknowledged and leaves one row; a segment is deleted only when every
   report in it is acknowledged; the writer drops the oldest sealed
   segments past 10,000 reports and counts them; `polariton report`
   refuses while the application's drainer holds `reporting.lock`; with
   the destination down the first-run and the kept figure complete
   unchanged and the reports land when it returns; a dead run's active
   segment recovered at the cap is admitted by the same rule as a writer's
   seal, and evicting a partially acknowledged segment counts only its
   unacknowledged reports as dropped; a missing progress file replays its
   segment, and one that fails any check - unparseable, over-long, bound
   to another segment or generation, or an in-range count altered by hand
   so that its checksum no longer matches - replays from zero and skips
   nothing, checked by killing the drainer between the temporary write and
   the rename and by each of those corruptions; a segment whose header
   names a revoked generation is deleted before any of its reports is
   attempted.
7. **The transition.** With segments queued, an older window still running
   and draining, and its writer still appending, a launch that reads
   reporting off takes the lock, waits for the attempt in progress, sends
   at most one final report within its bound, writes the disabled
   generation, and deletes the segments; the older window's writer stops
   at its next seal and removes its active segment, its drainer submits no
   new attempt after observing the generation, and the first-run path of
   the new launch never waited on the lock or the network. Afterwards the
   rows at the destination are a subset of the report ids that were ever
   sealed plus the final report's id, with at most one row per id; late
   execution of attempts submitted before the cutoff is permitted and the
   test allows for it by asserting on attempts submitted, not on rows
   arriving. A launch killed after the disabled generation is durable and
   before cleanup finishes, followed by a launch that re-enables reporting,
   leaves no report from the old generation recoverable: the old segments
   are deleted at the first launch that finds them, the new generation
   carries a new `install_id`, and no report from a revoked segment is
   submitted again; attempts submitted before the revocation may still
   execute, as decision 6 allows, and the test asserts on submissions, not
   on rows.
8. **The view over dogfood rows.** The operator's machines, enabled against
   the operator's workspace, produce rows; the business view published from
   its commands file shows them as separate journeys per run with the
   funnel's unknown column present, and the evidence states the
   dogfood-only scope.
9. **Both heads and the ladder.** The reduced head builds, the full
   Polariton suite and guards are green, and a guard asserts that no path
   in the reporting module can select a discovered workspace as a
   destination.

## Guardrails and stop conditions

1. If the first-run example needs anything a fresh installation does not
   have - a workspace, a file, a network - stop; that is the wrong example.
2. If any report column could carry the person's data, a path, or an
   identity, or if free text reaches `reason`, stop and reopen decision 5.
3. If a delivery failure or a queue write can delay or reach the person
   through anything but the log and the status detail, stop.
4. If the view is presented, anywhere, as a customer funnel before a page
   reports arrivals, that is a claim the evidence forbids.
5. Recheck the record number before the plan commit; two commits, plan then
   `impl:`, corrections amended under review, no push until stable.

## Risks

- **Two examples, two code paths.** The notebook example and the Rune
  showcase are both real product paths today; this record adds no third.
  The cost is that first-run explains why there are two, which is also the
  product's honest shape until Rune runs without a workspace.
- **Loss before persistence is real and bounded.** Up to 512 reports - the
  channel and one writer batch - die with a crash before `fdatasync`
  returns, and a torn tail in a dead run's active segment is discarded and
  counted; the record chooses that over a step that waits on a disk.
- **An attempt with an unknown outcome may still execute at the
  destination.** Stated in decision 6 and allowed for by gate 7: late
  executions leave one row per report id and can create no other; the
  drainer never advances past an unresolved report, and the cost is a
  slower drain, never a wrong table.
- **Disable is startup-only.** A running window keeps its channel until
  its writer's next seal or its drainer's next drain observes the new
  generation, then stops; the record says so, and the in-application
  toggle is a forward item once this transition is exercised.
- **A seeded dataset in a person's workspace.** Only after they started it
  and only the example's small table, named so it is obviously the
  example's; dropping it is one statement the closing cell shows.
- **The marker makes first-run a one-time event per state directory.** A
  person who wants it back deletes the marker; the closing cell says where
  it is.

## Forward

The discovery page and the download path, which give `journey_id` a source
and the funnel its first two steps. The per-visit receipt. The shadow. Rune
over the file adapters, so the first example can be the primary interface
without a workspace. An in-application reporting toggle, once the
transition above has been exercised through `init.rn`.
