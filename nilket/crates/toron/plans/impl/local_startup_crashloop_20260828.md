# Local deployment startup crash loop — 2026-08-24 → 08-28 incident report

The viz workstation's local deployment (profile `~/.local/share/nilket/local`,
systemd user unit `nilket-local.service`, supervisor `nilket local`, host
`optiplex`) was down for 3.7 days: **5,448 consecutive startup attempts**, each
killed by the supervisor's fixed 45-second readiness deadline while Wendelon
was ~45% through a recovery replay that takes ~100 seconds. Every number below
is from the journal or the preserved specimen unless marked as an operator
observation. Operator-visible symptom the whole time: every `ket.`-rooted
Polariton view failed (`failed · see status bar`) — Toron is only spawned after
Wendelon readiness, so the SQL surface never existed.

The loop was broken 2026-08-28 by raising the supervisor's `STARTUP_TIMEOUT`
45s→300s (ket `5ffcbcdd0`, viz-side stopgap — see "supervisor debts" for why it
is not a fix) and, separately, archiving the workspace and recreating fresh.
The archived workspace is a live repro specimen. Journal scope:
`journalctl --user -u nilket-local.service --since "2026-08-23"`.

## The arming sequence — ordinary operation walks into the loop

1. **A long-running session accumulates an inter-snapshot window.** The
   Aug 23 08:14 session ran ~20 hours under normal dogfood load. Its last raft
   snapshot sat at index 64999; applied state reached ~69,510 — 4,511 entries
   pending, *below* the next snapshot boundary (cadence is every 5,000 entries;
   see mechanism below). Nothing is wrong yet; this is the steady state the
   cadence produces.
2. **A dirty stop.** Aug 24 03:53:40: `systemd[11830]: nilket-local.service:
   Killing process 2437026 (nilket) with signal SIGKILL` — the final-kill phase
   of a unit stop, with no supervisor drain line preceding it. The pending
   window must now be replayed at next start.
3. **The loop.** Aug 24 14:41:11 first start attempt; 14:41:56 `Wendelon
   authenticated readiness timed out`. Kill at 45s lands mid-replay, so no
   snapshot, no purge, no progress of any kind — the next attempt faces
   byte-identical state. 5,448 cycles at ~51s each (45s deadline + drain +
   systemd restart) until Aug 28 08:11:15.

For contrast, the same workspace on Aug 23 reached `LOCAL_READY` in ~3 seconds
across several restarts — startup cost is not workspace age, it is the pending
replay window, which any dirty stop converts into startup debt.

## Mechanism — snapshot cadence sets a recovery window the deadline can't cover

Measured on the first two post-fix boots of the crashed workspace (spawn →
`LOCAL_READY`): 08:45:06→08:46:48 and 09:03:57→09:05:39, **~102 seconds each**,
replaying 4,511 and 4,613 entries respectively — ~45 entries/second.

The snapshot machinery is *not* wedged. The fresh workspace shows the normal
cadence: `snapshot-1` at index 4999, `snapshot-2` at 9999, purge advancing
behind them (→3999, →8999). Snapshots are built **every 5,000 applied
entries**. The crashed workspace's snapshot was pinned at 64999
(`snapshot_id: "recovered-64999"`, itself recovery-derived) simply because
applied (69,612) had not reached the 69,999 boundary.

Which means the up-to-5,000-entry pending window is the *designed* steady
state, and at the observed apply rate its replay costs up to ~110 seconds —
2.4× the old 45-second deadline. The crash loop was not an anomaly; it was
reachable from any healthy workspace via one dirty stop at the wrong point in
the cadence. Replay throughput is also data-dependent (entry size/shape), so
no fixed supervisor deadline is actually sound; 300s is a bet with margin, not
a bound.

## Contributing findings

**Dirty stops are produced by orderly shutdowns too.** Post-fix, a healthy
session's shutdown logged `draining Toron before Wendelon` (09:03:21) then
`escalating wendelon after 35s` (09:03:56) — the supervisor's
`WENDELON_DRAIN_TIMEOUT` (35s) expired and Wendelon was killed. Every such
escalation re-arms the replay debt the startup deadline then trips over. The
systemd unit's own stop timeout is a second, independent source of the same
SIGKILL (step 2 above). Drain deadline vs. flush time needs the same
reconciliation as startup deadline vs. replay time.

**Wendelon emits nothing during recovery.** Between spawn and the openraft
`get_initial_state` line (~101s into the crashed workspace's boot) the journal
is silent and the readiness probe just fails. Supervisor and operator cannot
distinguish "replaying, 60% done" from "hung". 5,435 of the 5,448 loop
iterations happened unattended partly because the failure mode is mute.

**Toron bootstrap hangs silently when storage is not ready** (operator
observation, 08-28): no error, no log line. Same diagnosability shape one ring
up, and the reason the supervisor's Toron health wait is equally blind.

**Wendelon maintenance runaway** (operator observation, 08-28, old workspace
before archiving): a maintenance task at ~355% CPU for 34+ minutes with the
data dir growing under it. The specimen preserves
`maintenance-staged-000000000000001e-0000000000000025.wnd` mid-flight. The
archived data dir is **2.0 GB**; the fresh workspace's control WAL reached
**92 MB (one segment, from index 0) within ~24 hours** of light dogfood use —
growth rate worth a look independent of the crash loop.

**A client-abandoned statement holds its tenant concurrency slot until the
server statement timeout** (operator observation, 08-28): one stuck query
wedged the tenant's concurrency on the old workspace; the CLI has a hardcoded
120s timeout and no flag, and `statement_timeout_seconds` defaults to 60 (the
operator raised it to 600 in the old dir's `toron.toml` only; the fresh
workspace regenerated with defaults).

## Supervisor debts acknowledged (viz lane, not db work)

The 300s stopgap sits atop wait loops in `nilket/crates/nilket/src/local.rs`
that swallow probe errors (`unwrap_or(false)` for Wendelon, `is_ok()` for
Toron), apply the deadline sequentially to both children (~10 min worst case),
and report no last error or phase on timeout — a terminal TLS/config failure
is indistinguishable from slow recovery for the full deadline. Viz side will
fix the diagnostics, but a defensible deadline *policy* depends on the
db-side decisions below (bounded replay, or startup progress the supervisor
can consume).

## Specimens

- `~/.local/share/nilket/local.crashloop-20260828` — 2.0 GB, the crashed
  workspace, untouched. **Also holds durable Polariton sessions; copy, don't
  consume.** Repro: point a supervisor at a copy — a 45s build re-enters the
  loop, a 300s build demonstrates the ~102s recovery.
- `~/.local/share/nilket/local.pre0548-20260812-1859` (310 MB) and
  `~/.local/share/nilket/local.restart-bug-backup` (162 MB) — the same dance
  on Aug 12 (restart counter 1). This is at least the third occurrence.

## Decisions this puts on the table (not taken here)

(a) bound recovery *time*, not entry count — snapshot cadence by replay-cost
estimate, or a startup deadline derived from the pending window; (b) recovery
progress reporting Wendelon-side, so supervisors and operators can tell replay
from hang (and the analogous silent Toron bootstrap); (c) drain deadline vs.
flush time, so orderly shutdowns stop minting replay debt; (d) tenant
concurrency slot release on client abandonment (plus a CLI timeout flag);
(e) whether readiness surfaces should expose recovery phase rather than
binary failure.
