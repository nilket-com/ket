# ADR 0169: opening a source and executing it are separate operations

Status: proposed 2026-09-12. Application lane; a bounded slice of ADR 0166
that completes ADR 0165's driver path. The batch driver (`polariton drive
<source> --commands <file> --receipt <file>`, PRODUCT-0158 §6) opens the
source and executes it before it reads the first command, so a script
that begins with `connect` (ADR 0166) arrives after the source has already
run against whatever connection the window had at startup. On a desktop
without a local workspace that means a Rune source cannot be driven at
all: the execution fails before the connection the script would have
established. A person has a choice the driver lacks: the window opens a
file, and although a Rune source runs on its own once a local workspace
attaches, the person can connect first and run afterwards, and can run
again.

## Decision

### 1. Two operations, each named

Opening a source loads it into the editor and describes it; executing it
runs it against the connection the window holds at that moment. They are
two operations on the command surface as they are two acts for a person.
`run_source` is the command that executes the open source, and its
receipt reports that execution's outcome: the figures it produced, or the
error the window reports for it.

### 2. Execution mode is explicit and reported

`drive` takes an execution mode. `--execute on-open`, the default, keeps
PRODUCT-0158's documented behavior: the source runs after opening and
before the first command, and every existing fixture and gate is
unchanged. `--execute by-command` opens the source and executes it only
where the script says `run_source`. The mode is chosen by the invocation,
never inferred from the script: appending or removing a `run_source` does
not change what happens before the first command. The receipt states the
mode (`source.execution: "on-open" | "by-command"`), and in `on-open` mode
a `run_source` in the script runs the source again, as a person would by
running it twice.

### 3. A run is one execution, and completion is that execution's

`run_source` starts an execution identified by the source buffer and its
generation, the database connection generation (ADR 0166 §3), and an
execution id the window mints. The command completes from that
execution's own outcome and nothing else:

- the run's figures and artifacts are attributed to the execution id, and
  the receipt reports only what that execution produced;
- a source edit after the run starts (a generation change) makes the
  execution stale: it is not reported as the run's outcome, and the
  receipt says the source changed;
- a connection replacement (a new database connection generation) makes
  the execution stale the same way, and the receipt says the connection
  changed;
- a second `run_source` while one is in flight is refused as overlapping,
  naming the execution in flight; the person's own run supersedes an
  agent's the same way, and the agent's receipt says so;
- a failed run after a successful one reports the failure and no figures;
  a successful run's figures never stand in for a later run's output;
- a run that does not settle by the command's deadline reports a timeout
  with the execution id, and a later settle of that execution is dropped,
  not delivered to a later command.

The headless driver's settle condition (scheduler idle, retained
relations complete, no error) becomes a property observed of one
identified execution, not of the window as a whole.

### 4. Interactive startup is unchanged

A window given a file at startup opens it and, where its documented
behavior runs it, still does. `run_source` over the live connection acts
on that window like any other command, under §3.

## This cut

`run_source` in the command vocabulary with the execution identity,
staleness rules, overlap refusal, timeout, and receipt of §3; the
`--execute` mode of `drive` with its receipt field; nothing else in the
window's startup. Rune sources only; markdown notebooks already run cell
by cell through `run_cell`.

## Acceptance

1. A headless test drives a Rune source in `by-command` mode with
   `[connect, run_source, describe]`: nothing executes before `connect`
   (no figures, scratch untouched), `run_source` reports the figures that
   execution produced, and `describe` lists them. The same source in the
   default mode with `[describe]` executes on open as before, with
   `execution: "on-open"` in the receipt; every existing driver fixture
   passes unchanged. `[connect, describe]` behaves identically whether or
   not a `run_source` is appended.
2. A `run_source` with no Rune connection is refused naming the missing
   connection, before any execution; a `run_source` whose execution
   errors reports the error and no figures, including immediately after a
   successful run.
3. Tests for §3 under simulated concurrency: a source edit during a run,
   a connection replacement during a run, an overlapping `run_source`,
   and a run past the deadline each produce the receipt §3 names, and none
   reports the earlier execution's figures. A late settle of a stale or
   timed-out execution changes no later receipt.
4. From the Windows desktop, `polariton drive --execute by-command
   uat/plot_trends_showcase.rn` with `[connect (Bigfoot tenant 1),
   run_source, publish]` produces a package whose figures are the
   showcase's views and whose dataset provenance names Toron. That run is
   ADR 0165's driver path and is recorded in ADR 0165's evidence; the
   rendering is the showcase's claim, not this ADR's.

## Limits and follow-up

`run_source` does not select which figures to run or accept arguments; a
partial run is a separate decision. A source whose execution needs a
local workspace still needs one; this ADR changes when execution happens
and how its completion is attributed, not what it needs.
