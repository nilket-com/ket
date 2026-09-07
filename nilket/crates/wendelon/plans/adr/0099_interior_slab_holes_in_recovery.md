# ADR 0099 - Interior slab holes in recovery

Status: proposed 2026-09-06.

## Context

A slab append is two steps: the slab reserves a page-aligned range by
advancing its cursor, then the writer fills the range and, for a durable
append, fdatasyncs the file. ADR 0098 decision 4 moved the reservation under
the manager's rotation lock and made it a token the write consumes. A
reservation whose write never lands - the request future dropped on a client
disconnect, an I/O error on the write - leaves its range as it was: never
written. The next append reserves past it, writes, syncs, and is acknowledged.
The kernel extends the file over the gap with zeroes.

Recovery walks the slab from offset zero and stops at the first page whose
header carries no record magic, treating it as the tail: never-written
zeroes or a torn record. An interior hole therefore ends the scan early.
Every record after it - acknowledged durable to a client - is absent from the
rebuilt index, and the recovered write cursor sits at the hole, so the first
append after restart overwrites the first of those records. A torn region
with bytes in it - a write that failed part-way, ahead of a record that was
later acknowledged - does the same. The tiered path walks sealed bytes with
the same rule and loses the same records.

`durable_append_after_an_unwritten_reservation_survives_reopen` in the
manager tests reproduces it: reserve A and drop the token, append B durably,
reopen, and B is gone. The test was red before this record.

The first draft of this record stepped over any zero page. Review showed
that a zero page is not evidence of a reservation boundary: a multi-page
write whose first page did not persist leaves a zero page in front of
persisted payload pages, and a walk that steps over the zero page lands
inside that payload. If the payload happens to hold a byte-exact record at a
page boundary, the walk indexes it. Recovery would have invented a record.
The design needs durable evidence of where records start, and the node
already has it.

A durable append acknowledges in this order: write, fdatasync through the
group-commit driver, then write the record's row to the redb storage index
with an immediate commit, then reply. A row in that index therefore names an
offset whose bytes were persisted in full before anyone was told about them.
The index is opened before slab discovery, and its snapshot is already
loaded there for staged-maintenance reconciliation.

The defect predates 0098. The reserve-then-write pair was always one future
and always cancellable between its halves. 0098 only made the gap easy to
see.

## Decision

### 1. Open-time recovery resumes at acknowledged offsets and canonicalizes the file

`recover_slab` takes the set of acknowledged record offsets for the slab,
built from the durable index rows the manager loads before it opens any
slab. The walk validates records as before: magic, a length that fits the
file, and a checksum. Anything else is an anomaly - a zero page, a page with
bytes but no magic, a record that runs past the end of the file, a torn
record - and on an anomaly the walk moves to the smallest acknowledged
offset beyond it. If there is none, the anomaly is the tail. A record found
at an acknowledged offset still has to validate; one that does not is
skipped the same way, never trusted because a row says so.

The recovered write cursor is the end of the last record that validated.
Every region the walk stepped over, and whatever trails the last accepted
record, is zeroed and the file is synced before the slab opens for writes.
After open, a slab file holds accepted records and zero pages and nothing
else.

Without the durable index - it failed to open - there is no oracle and the
walk stops at the first anomaly, as it did before this record. Appends are
still acknowledged in that mode - the strict row write is skipped when there
is no index - so this is the pre-record exposure, not a safe mode. The node
warns at open that the index failed, and that warning is the signal to act.

### 2. A failed write zeroes its reservation

The framed write is submitted on its own task, so dropping the request
future mid-flight cannot strand a partly written range: the task runs to
completion. On an error or a short write it zeroes the whole reservation
before reporting the failure. A reservation whose write was never submitted
is zero by construction. This is hygiene, not a soundness argument: it keeps
live files close to canonical so that the walks below rarely need their
evidence, but nothing in this record depends on the zeroing having
succeeded. A zeroing that fails is logged at error level.

### 3. The sealed-bytes walk takes the same evidence, and it is mandatory

`recover_slab_entries_from_blob` takes the set of acknowledged offsets for
the slab as a required argument and applies the rule of decision 1: on an
anomaly it resumes at the next acknowledged offset and stops if there is
none. There is no zero-page skip. A zero page proves nothing about the pages
after it, and the second draft's assumption that every sealed byte string a
live process reads is canonical did not hold: blobs tiered before this
record never passed through open-time canonicalization, and a failed zeroing
under decision 2 can leave exactly the torn-first-page shape in a live file.

The evidence is the manager's own index for the slab: for a tiered slab its
offsets were loaded from the durable rows, and for a local sealed slab they
came from open-time recovery plus live appends since. The manager exposes
them per slab and for all slabs at once, and every one of the eleven callers
passes them, including the tiered decoder, whose tenant check walks the
decrypted bytes before anything else does and would otherwise refuse a blob
the node itself sealed with a leading hole. Two callers walk bytes the
process just rewrote from validated entries and pass the empty set, which is
the strict walk: rewritten bytes have no holes, and an empty set can never
add a record. That is the property
that makes a mandatory argument safe to thread everywhere: evidence can only
admit records the manager already knows, never invent one.

## Acceptance

- Reserve a three-page frame on the active slab and drop the token; append a
  record durably; reopen the manager. The record's location and bytes are
  recovered, and an append after reopen lands past it and leaves its bytes
  intact. Under the first-anomaly rule the lookup returns nothing.
- A record whose payload carries a byte-exact copy of another record at its
  second page, with its first page zeroed after the fact: reopen recovers
  the original, promotes nothing at the copy's offset, and zeroes the torn
  region. Under a zero-page skip the copy is indexed as a second version.
- A record whose magic is overwritten with non-zero bytes, ahead of an
  acknowledged record: reopen recovers the acknowledged record, zeroes the
  torn region, and appends past it.
- With the index path occupied so redb cannot open, a hole followed by an
  append: reopen finds nothing after the hole.
- Sealed bytes with an interior zero page, and the same bytes with a torn
  page in the hole: with the slab's acknowledged offsets the blob walk
  recovers both records in both cases; with the empty set it stops at the
  first anomaly in both cases.
- The reviewer's direct-bytes reproducer - a torn record whose zero first
  page precedes a persisted payload page holding a byte-exact record - fed
  straight to the blob walk with the slab's real evidence, and with none,
  promotes nothing.
- A sealed slab whose first reservation was dropped encrypts with the
  manager's evidence and decodes with it; without evidence the decoder
  refuses it. Under the third draft the decoder passed the empty set and
  refused the node's own blob.
- A slab whose file ends in zero pages recovers its records and resumes
  appending at the last record's end.
- `cargo test -p wendelon --lib -- storage::manager::tests storage::core
  storage::maintenance_policy`, `--test tombstone_compaction`,
  `--test migration_sweeper`, both architecture tests, and
  `cargo check --workspace --all-targets` are green.

## Guardrails

- Recovery never invents a record. After an anomaly it accepts a record only
  at an offset the node itself wrote to the durable index after persisting
  the bytes, and only if the record validates there.
- The torn-record rule at the tail is unchanged. A page that was partly
  written and has no acknowledged record after it still ends the scan.
- No filler is written when a reservation is dropped. The failing write path
  zeroes its own range; the cancelled path never touched it.
- The durable index is the oracle, not the index. The authoritative location
  index is still rebuilt from slab bytes, and the row snapshot is rewritten
  from it after recovery as before.

## Risks

- The first draft's zero-page skip in the file walk was unsound and was
  committed locally before review caught it. It never reached origin.
- Zeroing at open writes once per skipped region and syncs once. A dropped
  2.6 MB frame at the PRODUCT-0157 fixture's size is one write.
- The second draft assumed sealed bytes were canonical and let the blob walk
  step over zero pages without evidence. Review showed two ways that input
  reaches the walk uncanonical: blobs tiered before this record, and a failed
  zeroing under decision 2. Both drafts were committed locally and neither
  reached origin.
- A write that fails and then fails to zero its range leaves a torn region
  in a live file. Every walk over it, live or at open, now has evidence and
  resumes at the next acknowledged record, so the region costs nothing but
  space until a rewrite drops it.
- The per-slab evidence is built by scanning the whole key index. The
  callers that walk many slabs build it once for all; the others pay one
  scan per walk on maintenance and tiering paths that already read the
  whole slab. The root cap of the manager moved to 4,650 lines for the
  plumbing; the free functions that take a blob are the next carve.
- A record after an anomaly that the index does not know is dropped by the
  walk. Such a record is unreadable already - the index is the read path -
  so nothing observable is lost, but a rewrite drops it physically.
- A non-durable append writes its row without a preceding fdatasync. Its
  offset can appear in the oracle with bytes that never persisted; the walk
  skips it because it does not validate. A non-durable append that persisted
  but crashed before its row is accepted before any anomaly and skipped
  after one. Neither carried a durability promise.

## Forward

The hole itself stays on disk until the slab is rewritten or retired; a
maintenance rewrite drops it naturally because the rewritten slab is packed
from recovered entries. The per-slab acknowledged set is built from the
whole row snapshot at open; if row counts make that a measurable cost, a
per-slab high-water mark written once per group commit would give the same
evidence in one integer.
