# ADR 596 - q21's replay-1 build write on strix: lifecycle decomposition of `inner_1_build_write`

**Status:** CLOSED, BELOW FLOOR. One run on strix under the separated
launch (results in
`plans/impl/0596_q21_inner_1_build_write_decomposition_results.md`):
separation and window verified, off range 324.1 ms (3.61%) inside the
cutoff, the seventh band reconciled and consistent on every pair, and
no child clearing the 648.2 ms floor (nor 500 ms): the leader is
`build_write_index` at 482.4 median-scaled ms (54.9% of the write,
5.37% of q21), then `build_write_scan_wait` at 179.0. Facts: the build's
scan input is an Exchange of four partitions with no filter, rows
written whole, 5,860 pages of about a thousand rows, legacy index. No
owner, no cut; the successor is chosen from the shape. Successor to ADR
595, which resolved the replay-1 preparation's owner to
`inner_1_build_write` under a verified separated launch.


## Context

ADR 595 read `inner_1_build_write` at 876.4 median-scaled ms, 9.73% of a
9,005.8 ms off median, resolved 613 ms ahead of the partition sizing on a
0.51% off range; ADR 594's steady pairs had read it at 877-885 ms. Those
readings are historical context for this record; the instrument below is
a new executable and its own unprofiled arm is its control.

What the term contains, at `e3ef86bd`. ADR 594 clocks `inner_1_build_write`
around the spilled branch of `prepare_reoriented_inner_join_runtime_with_scan_build`
for `JoinReplayId(1)`: from the instant before
`SpilledInnerJoinBuild::create_with_resident_baseline` to the instant
after `spilled.finish` returns, on the path q21 takes at SF=8 under 2 GiB
(`partition_count = 1`, 5,845,938 build rows, 27,242,808 spill bytes,
ADR 594/595's facts). Inside that span the work is sequential on one
task, so its pieces do not overlap and can be named exclusively:

1. **Create.** `create_with_resident_baseline`: the spill file is created
   under the join-spill directory and the page key is set up.
2. **The batch loop**, in `write_streaming_scan_inner_build_batches`. The
   scan is opened (`open_scan`, awaited once), then per batch the stream
   is polled (`stream.next().await`), the row limit is applied
   (`limit_streaming_scan_batch`, which can end the loop before a final
   `None` is ever observed), the filter predicate is applied when the
   scan input carries one (`filter_batch_with_bound_predicate`, q21's
   `o_orderstatus = 'F'`; a batch it empties is skipped), and, when the
   build's scan input carries an Exchange, `repartition_exchange_batch`
   splits the batch by hashed key into one batch per partition (with one
   partition, a hash per row into one bucket) and each non-empty piece is
   written; with no Exchange the batch is written whole. Whether q21's
   build input carries an Exchange is not on record and is a fact the
   instrument records. The caller's awaits are sequential, so the spans
   the instrument takes around opening and polling are disjoint wall
   spans of the caller; they say how long the caller waited on the scan,
   not how the read and decode work behind the stream divides between
   CPU and I/O or whether all of it ran inside those spans.
3. **Per chunk of up to 8,192 rows of each incoming batch** (the chunks
   restart at every batch, so the chunk count is at least
   `ceil(rows_kept / 8192)` and can exceed it, more so with an
   Exchange), in `write_projected_batch_chunks` when the build keeps a
   column subset or `write_batch_chunks` when it keeps every column: the
   chunk is projected to the retained columns (`project_batch_columns`,
   only on the projected path), then `write_page_batch_inner` runs its
   steps in sequence: **key extraction** (`key_arrays` and the
   `join_key_at` loop into the scratch refs); then, only if the chunk
   yielded at least one keyed row, **IPC encode** of the chunk, the
   **page write** (`write_join_spill_plaintext_page_returning_buffer`:
   AEAD seal in place, then the file write, awaited), and **indexing**
   (the per-row insert into the compact index, or the legacy append,
   then `try_compact_near_cap_resident_index` and the resident guard). A
   chunk with no keyed row returns after key extraction and emits no
   page.
4. **Finish.** `spilled.finish`: `set_len`, `fdatasync`, and the resident
   guard.

The existing `GroupedChainSpillObserver` timers around key extraction,
encode, and encryption belong to another route's profile and are not
armed on this path (`write_page_batch` passes no observer); this record
adds its own accumulation and leaves them alone.


## Decision

**A seventh band, additive, inside `inner_1_build_write`.** Under the
profiling handle, the spilled build's writer carries a
`BuildWriteLedger`: a local accumulator of microseconds and call counts
for eleven children, threaded as an `Option<&mut BuildWriteLedger>` through
`write_streaming_scan_inner_build_batches`,
`write_streaming_scan_inner_build_batch`, `write_projected_batch_chunks`
(and its unprojected sibling), and `write_page_batch_inner` on this path
only, `None` everywhere else so no other route is touched, and published
once at the end of the branch. Each child is one span per call of the
code it names, disjoint from the others by the sequence above:

```
inner_1_build_write
  build_write_create        create_with_resident_baseline, once
  build_write_scan_open     open_scan, once, awaited
  build_write_scan_poll     each stream.next().await, one span per poll
  build_write_filter        each filter_batch_with_bound_predicate (zero calls without a filter)
  build_write_repartition   each repartition_exchange_batch (zero calls without an Exchange)
  build_write_project       each project_batch_columns per chunk (zero calls on the unprojected path)
  build_write_key_extract   key_arrays and the join_key_at loop per chunk
  build_write_encode        encode_record_batch_ipc_into per emitted page
  build_write_page_write    seal and file write per emitted page, awaited
  build_write_index         the row insert loop, compaction check, and resident guard per emitted page
  build_write_finish        spilled.finish, once
  build_write_residual      inner_1_build_write − sum of the eleven, computed at close
```

`scan_open` and `scan_poll` are reported beside each other and summed
as `scan_wait` for the rule; both are the caller's wall spans.

The residual is everything else in the span: chunk slicing, the row
limit, empty-batch skips, budget checks, the bookkeeping between steps,
and the instrument's own cost. The children are clocked at different
frequencies — once per build (create, open, finish), once per poll,
once per batch (filter, repartition), once per chunk (project, key
extraction), once per emitted page (encode, page write, index) — so the
number of clock reads is arithmetic from the measured counts, stated in
the results from the facts, and the instrument's cost is what the
perturbation gate and the residual show, not a figure asserted here. A
negative residual is a defect the receipt reports.

**Facts follow the branches taken, and each supported shape is
validated or the pair is unsupported.** Beside the terms, under the same
gate, the path facts: `build_write_filter_present`,
`build_write_exchange` (`true`/`false`, with
`build_write_exchange_partitions` when true), `build_write_projected`
(`true` for the retained-column path, `false` for the whole-row path),
`build_write_termination` (`stream_end`, a final `None` observed, or
`row_limit`, the limit reached with no final poll), `build_write_index_mode`
(`compact` or `legacy`); the counts: `batches_read` (polls that
returned a batch), `batches_filtered_empty`, `batches_written` (batches
or Exchange pieces handed to the chunk writer), `rows_read`, `rows_kept`,
`chunks`, `chunks_unkeyed` (chunks with no keyed row), `pages`,
`plaintext_bytes`, `ciphertext_bytes`, `index_entries`,
`index_compactions`; and every child's call count. A child that did not
run on the path taken has zero calls and zero time and is a fact; a band
whose terms or facts are absent while `inner_1_build_write` is non-zero
is incomplete. Consistency, checked and named, each rule conditional on
the facts it depends on: `create`, `scan_open`, and `finish` calls are 1;
`scan_poll` calls equal `batches_read + 1` under `stream_end` and
`batches_read` under `row_limit`; `filter` calls equal `batches_read`
when the filter is present and 0 when not; `repartition` calls equal
`batches_read − batches_filtered_empty` when the Exchange is present and
0 when not; `project` calls equal `chunks` on the projected path and 0
on the unprojected one; `key_extract` calls equal `chunks`; `encode`,
`page_write`, and `index` calls each equal `pages`; `pages` equals
`chunks − chunks_unkeyed`; `rows_kept` equals ADR 594's
`inner_1_build_rows`; `ciphertext_bytes` equals
`inner_1_build_spill_bytes`. The supported shapes are the four
combinations of filter present or absent and projection present or
absent, with or without an Exchange, under either termination; a pair
whose facts name a shape outside that set (a preserved-rows write, an
observer-profiled writer) is reported `withheld_unsupported_path`, not
read through the band.

**Gated.** ADR 594's and 585's clocks are unchanged. The ledger exists
only under the profiling handle; the off arm takes none of these reads;
an off-arm fixture proves the writer path records nothing and the band
is absent.

**Timing algebra, seven bands.** ADR 594's identity stands, and:

```
sum(eleven children) + build_write_residual = inner_1_build_write
```

reconciled on every on row; completeness per pair as ADR 594 fixed:
residual at most `overhead_i + 0.05 × inner_1_build_write_i`, consistent
facts, no missing child.

**Everything else carried forward unchanged**: ADR 591's coordinate and
binding, ADR 593's readings, journal collection from the launch instant,
scope, start gate, and 4% cutoff, ADR 595's separated launch with its
verified separation interval and clean window as preconditions for
reading an owner, one instrumented release executable built on bigfoot,
one warmup and five alternating pairs, ADR 584's perturbation gate.

**Statistic and rule, over the terms.** The rule's universe is
`scan_wait` (open plus poll), the nine other children, and the residual:
eleven terms. ADR 594's scaling, median,
floor `max(500 ms, 2 × range_off)`, and outcomes — `resolved`,
`unresolved_tie_within_off_range`, `below_floor_no_mechanism`,
`withheld_instrument_incomplete` (a residual that leads at any size, or
inconsistent facts), `withheld_incomplete_profiles`,
`withheld_unsupported_path` (the partitioned branch ran), and
`closed_on_variability` — with ADR 595's `withheld_launch_separation_*`
and `withheld_window_not_clean_*` taking precedence. Below floor is a
live outcome here: an 880 ms term split ten ways may have no child above
500 ms, and that is the finding, not a failure.

**The mechanism floor, priced as gross candidate work and not acted
on.** If a child resolves, the subterm a mechanism would remove is named
and priced pairwise as ADR 585 fixed, and labelled gross: for the scan
wait, the polls a carried or cached input would not repeat (rows the
sizing scan and the partition-sizing pass already read); for encode plus
page write, the pages a resident build would not write; for indexing,
the inserts a once-built index would replace (ADR 589's pattern). A
gross figure is what the candidate would stop doing, not what it would
save: a resident or carried input still has to be allocated, accounted
against the memory budget, and paid for on the drive side, and only a
paired net-gain measurement under its own record says whether any of it
is a gain. Whether the gross subterm clears the floor is reported; no
cut opens under this record.

**Attribution only, one attempt, every outcome retained.**


## Acceptance

- The ledger records each child once per call with the counts named,
  reconciled on a fixture receipt against `inner_1_build_write`; the
  band is absent on an unarmed profile; a negative residual and a
  partial band are defects; each consistency rule has a fixture that
  breaks exactly it (a repartition call without an Exchange, a filter
  call without a filter, a project call on the unprojected path, poll
  counts against the wrong termination, chunk counts that disagree,
  pages above `chunks − chunks_unkeyed`, rows or spill bytes that
  disagree with ADR 594's facts), and an unsupported shape is reported
  as such.
- Writer fixtures through the real path: projected and unprojected
  input, a scan input with and without a filter, an all-unkeyed chunk
  (key extraction counted, no page emitted), and a row-limit termination
  (no final poll), each yielding the facts and counts the rules expect;
  and the path without a ledger preparing the same spilled build (rows,
  pages, spill bytes) as with one while recording nothing.
- The driver returns each outcome on fixtures, with the eleven-term table
  and the pairwise pricing of the resolved child's removable subterm.
- One run under ADR 595's launch: eleven rows admitted, readings present,
  separation and window verified from the driver's own collection, seven
  identities reconciled on every on row, facts consistent, the cutoff
  verdict, and the eleven-term table with the rule's outcome; or the
  retained refusal with its cause.
- Plan commit, then one implementation/results/closure commit.


## Guardrails

- The children are the call sites named above at `e3ef86bd`; the record
  is re-grounded if the path moves before the instrument lands.
- No change to the writer, the spill format, the index, the scan, or any
  route; the ledger is a parameter that is `None` off this path.
- Build on bigfoot, measure on strix, the separated launch, nothing else
  on strix in the window.


## Risks

- The scan spans are the caller's waits on opening and polling; what the
  stream did behind them, and where, is not decomposed by this
  instrument, and the results say so.
- The clock reads scale with polls, batches, chunks, and pages; their
  cost is read from the perturbation gate and the residual, not assumed.
- Below floor is the most likely outcome for a term this size; the record
  then names the shape (which children share it) and the successor
  chooses between deeper instrumentation and a mechanism priced across
  children.


## Forward

- A resolved child names its record, each a candidate with a gross
  figure to be netted under its own paired measurement: the scan wait
  toward a carried input, the page path toward a resident build under
  the budget, the index toward ADR 589's once-built pattern. Below floor
  names the shape; the next record is chosen from it, not assumed here.
