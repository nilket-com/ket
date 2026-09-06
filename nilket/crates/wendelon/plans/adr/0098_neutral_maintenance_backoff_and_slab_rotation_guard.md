# ADR 0098 - Neutral maintenance backoff and slab rotation guard

## Context

The local deployment pinned one core continuously after the PRODUCT-0157 browser
fixture landed a 1,157,625-row `CREATE TABLE AS` into Wendelon. Nothing was
failing: `journalctl` was silent, no receipt was written, and the deployment
answered queries. `/metrics` told the story instead.

- `wendelon_maintenance_attempts_total{result="completed_without_rewrite"}`
  advanced at about **4 per second** and stood at **152k after one day**.
- `storage_compression_decode_micros_count` advanced at **350-460 per second**;
  lifetime decompressed bytes reached **500 TB** against a store measured in
  gigabytes. Disk I/O was nil: every sealed slab was resident in the page cache,
  so the work was pure CPU.
- One tokio worker stayed pinned; `/proc/<pid>/task/*` samples put it inside the
  retention scan's frame decode.

The mechanism is ADR 0097 decision 4 doing exactly what it says. Every 5 s the
migration sweeper calls `reclaim_mvcc_retention_once(1)` and the tier sweeper
calls `reclaim_tombstoned_ranges_once(1)`. Both loops walk **every** sealed slab
and run one policed attempt per slab. `max_slabs = 1` bounds the number of
**rewrites** per pass: `report.compacted_slabs` only advances on
`scan.compaction.is_some()`, so a pass in which no slab needs a rewrite attempts
all of them. Each attempt materializes the slab's records to decide there is
nothing to prune, returns `SuccessDisposition::Neutral`, and `record_neutral`
finalizes the reservation and records nothing else - "the next attempt is still
the next attempt". Five seconds later it is. With 21 sealed slabs on the node,
one of them 450 MB, that is a full re-decode of the store every tick, forever.

The 450 MB slab is the second defect. `slab_max_bytes` is 64 MiB.
`target_for_append` rotates when `write_cursor() >= max_slab_bytes` and admits
the append otherwise, without looking at the payload. The CTAS wrote 135
multi-row frames of about 2.64 MB each; each landed while the cursor was still
under the cap, and the slab grew to seven times it. A slab that large is the
worst possible input to a scan that re-decodes whole slabs on a timer.

ADR 0582 closed the quarantine control surface with one item deliberately left
open: deterministic attempts one and two are durable backoff receipts that the
report grammar cannot express, and the record says to add a distinct
`BackingOff` variant rather than misreport them as retryable races. This cut
touches the same report page and closes that item.

Containment is applied on the local profile: `[time]
mvcc_retention_window_secs = 0` in the local `wendelon.toml` disables the
retention scan and took the daemon from 110% to 11% of a core. The tombstone
pass still runs the neutral loop over the small slabs. Containment is not a fix.

## Decision

### 1. A pass budgets attempts, not rewrites

`prune_versions_for_retention` and `reclaim_tombstoned_ranges` count every
**completed** attempt against `max_slabs`, whether or not it rewrote. A refused
slab (`MaintenanceAttempt::Skipped`) costs nothing and the walk moves to the next
candidate. With the sweepers' `max_slabs = 1` a tick decodes at most one slab.

The unpoliced paths keep their rewrite budget. `reclaim_table_drop_storage_slice`
already bounds itself by a durable cursor (ADR 0095) and does not revisit a slab
it has passed; `prune_versions_older_than` and `reclaim_table_drop_storage` are
test-only entry points. Budgeting attempts there without a cooldown would pin a
pass to its first slab every tick, which is the opposite of progress.

### 2. A neutral completion cools the slab set down

A completion that **scanned** a slab set and found nothing to rewrite records a
neutral cooldown for `(operation, canonical slabs)`: `NEUTRAL_COOLDOWN_BASE`
(5 s, one sweeper tick) doubling per consecutive neutral completion to
`NEUTRAL_COOLDOWN_CAP` (300 s). Admission refuses the set until the deadline
with a distinct `AdmissionRefusal::NeutralCooldown`, counted as
`result="skipped_neutral_cooldown"` on the attempt counter. A rewrite clears the
set's neutral cooldown; a failure replaces it with the failure's own schedule.
The cooldown is ephemeral, like the retryable cooldown: a restart forgets it and
the first tick after restart scans once more.

A completion that never **owned** the slabs - the claim was refused because the
slab was pinned, retired, or held by another job - or that found work a pin
gate deferred, proved nothing about whether the set is clean and records no
cooldown. `SuccessDisposition` gains `Unclaimed` for both; `Neutral` now means
"scanned and clean". The per-slab attempt bodies report which one happened,
from the claim's answer and from whether the scan marked any record reclaimable.

Together with decision 1, a node with N sealed slabs and nothing to prune scans
one slab per tick and each slab at most once per cooldown period, converging on
one decode per slab per five minutes instead of N decodes every five seconds.

### 3. Backing-off work is reported as such

`MaintenanceReportItem::BackingOff { operation, slabs, origins }` carries the
deterministic receipts whose `next_eligible_at_ms` is set. `report_page` emits
them after the quarantined items and before the retryable cooldowns, with their
own cursor kind, so a page boundary between a work identity's quarantined and
backing-off views loses neither. The wire mirror gains the same variant in the
control server, the client crate, and the Toron maintenance CLI, which prints
`backing_off` lines with each origin's deadline.

### 4. Append rotation is payload-aware

`target_for_append(payload_bytes)` admits the active slab when the cursor is
under the cap **and** either the slab is empty or the payload fits under the
cap. Otherwise it rotates first. A payload larger than the cap is still admitted
into an empty slab: the cap bounds overshoot to one payload, never refuses a
record. Every append path passes the frame length it is about to write.

## Acceptance

- With three sealed slabs and nothing prunable, one retention pass with
  `max_slabs = 1` records exactly **one** neutral cooldown; the next pass records
  the next slab's, and a pass after all three are cooling attempts nothing. The
  assertion is on the policy's cooldown table, which a rewrite-only budget cannot
  satisfy: it would leave three entries after the first pass.
- A neutral completion is refused on re-admission until the clock passes its
  deadline; consecutive neutral completions double the wait and stop at the cap;
  a rewrite clears it. The refusal has its own label and the attempt counter
  renders it.
- An unclaimed attempt (a pinned slab) records no cooldown: the existing
  pin-deferral tests re-run their pass immediately and still see the rewrite.
- One deterministic failure yields a `BackingOff` report item, not a
  `QuarantinedWork` item; three yield `QuarantinedWork` and no `BackingOff`. A
  one-item page walks quarantined, backing-off, retryable, and in-flight items in
  that order without skipping or repeating one.
- Three appends of 600 KiB against a 1 MiB cap land in three slabs. Under the
  cursor-only rule the second append lands in the first slab.
- `cargo test -p wendelon --test tombstone_compaction`,
  `--test migration_sweeper`, the `maintenance_policy` and `manager` unit tests,
  `cargo test -p toron cli::maintenance`, both architecture tests, and
  `cargo check --workspace --all-targets` are green.

## Guardrails

- A neutral cooldown is not evidence about receipts. It never touches the
  deterministic map, never counts toward quarantine, and never appears in the
  quarantine fingerprint.
- The budget change does not alter what a rewrite pass does: a pass that finds
  work still rewrites up to `max_slabs` slabs. It only stops a pass that finds
  none from decoding the whole node.
- The rotation guard never refuses an append and never splits a frame. It moves
  the rotation decision from "after the cap was crossed" to "before it would be".

## Risks

- A tombstone or a retention-floor advance that makes a cooling slab reclaimable
  waits out the remainder of that slab's cooldown, at most 300 s. Reclaim is
  already asynchronous and bounded by the tick; five minutes of delay is accepted
  against a full-store decode every five seconds.
- Rotating before a large payload leaves the previous slab short of the cap. At
  the fixture's 2.64 MB frames the loss is under 4% of a 64 MiB slab.
- `BackingOff` is a new wire variant. An old client reading a new node's report
  fails to decode the page while any origin is backing off. The report is an
  operator surface behind `toron maintenance`, deployed with the node.

## Forward

The neutral cooldown table is bounded by the slab sets scanned within two cap
periods per operation: an entry outlives its deadline by one cap period, so the
next clean scan of a live set still doubles from it, and is swept on the next
neutral record after that. If a deployment shows the table growing, key the
sweep on slab retirement instead. The 450 MB slab itself stays on disk until a
rewrite or reclaim retires it; nothing here rewrites a slab that has nothing to
prune.
