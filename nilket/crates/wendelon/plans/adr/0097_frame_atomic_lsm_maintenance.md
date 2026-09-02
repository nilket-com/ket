# ADR 0097 - Frame-atomic LSM maintenance

## Context

The local deployment OOM-killed five times in four hours, every 17-36 minutes, at
44-54 GB peaks. Wendelon held 48 of the unit's 50 GB; Toron held 28 MB. Compaction
succeeded **zero** times in eight hours against 49 x `LSM temp-output writer does
not yet split multi-row frames`, 9 x `missing LSM source frame for planned
survivor` and 6 x `inconsistent LSM source frames for the same physical record`.

This is not a startup or readiness defect. Every start reached `LOCAL_READY` in
14-32 s, and `nilket local` gates that line behind a real
`blocking_health_loopback` against the SQL endpoint, so it cannot announce a port
it has not proven. (The SQL listener is QUIC, so it is a UDP socket: `ss -ltn`
shows nothing on 7742 and the endpoint is nonetheless healthy. An early reading in
this investigation called the deployment unserving on exactly that mistake.)

The store is **3.2 GB on disk against 48 GB resident**. The amplification is in
memory, not an on-disk backlog, and it has three compounding sources.

**Per-row charging of a physical frame's length.** `storage/lsm.rs:397` packs a
compaction run by `record.location.length`, which for a multi-row frame is the
length of the whole containing frame, charged once per logical row it holds. The
observed slab holds **8,428 physical frames and 47,050 logical rows** in 2.20 GiB:
the planner accounts for it as **18.81 GiB**.

**Per-row reads of a shared frame.** `manager/lsm_driver.rs:1248`,
`read_lsm_single_row_source_frames`, sizes its buffer with `entries.len()` - the
logical row count - and reads through `read_with_reference_origin` once per entry.
Rows sharing a physical frame each retain a full copy of it. The writer then
clones one copy per unique physical frame on top, for a working set of at least
**21.01 GiB**.

**Retry without backoff after a deterministic failure.** Having built that working
set, the run returns the known `ErrorKind::Unsupported` and retries after 100 ms.
The failure is deterministic in the slab's shape, so the next attempt rebuilds the
same working set and fails the same way. Concurrent retention rewrites the same
slab, which produces the missing-source and inconsistent-frame errors, and that
loop retries on a 5 s period. RSS ratchets rather than returning to baseline:
21.8 GB after the first cycle, 40.9 GB after 19 minutes.

None of this is the ADR 0055 slab-pin saga returning. The installed binary
(2026-08-26) contains `da2117e1`, and no Wendelon commit has landed since.

## Decision

Maintenance is planned, read and accounted in units of **physical frames**, and a
deterministic failure must not be retried as though it were transient.

### 1. Output planning and size accounting are frame-atomic

A compaction run's unit of work is the physical frame. A frame's length is charged
once, not once per logical row it contains, and a run either takes every surviving
row of a frame or does not take the frame. This removes the case whose only
handling today is an `Unsupported` error after the working set has been built.

### 2. One read and one decode per physical frame, shared across its rows

Source frames are read per physical frame and shared by every row that resolves
into them. The retained working set becomes proportional to the frames a run
touches, not to the rows those frames contain.

### 3. One maintenance ownership claim spans a whole job

An LSM or retention job claims the slabs it will rewrite for the duration of the
job, so a concurrent rewrite cannot invalidate a plan mid-flight. The
missing-source and inconsistent-frame errors are consequences of overlapping
claims and are not to be handled by retrying into the same race.

### 4. Deterministic failures back off and quarantine

A failure that is a function of the input's shape is not retried on the transient
schedule. It backs off, and after a bounded number of attempts the slab is
quarantined out of the maintenance rotation with a receipt, so a permanently
unhandleable slab costs a bounded amount of work rather than an unbounded loop.

### 5. Slab admission and maintenance replacement are bounded

A maintenance job declares the working set it will hold before it allocates it,
and a job whose declared set exceeds the node's maintenance budget is refused
rather than attempted.

## Acceptance

- A slab of 8,428 frames / 47,050 rows plans to its **physical** size, not to
  18.81 GiB; the assertion is on the planner's accounting, not on wall time.
- A compaction run over a multi-row-frame slab retains bytes proportional to the
  frames touched. The discriminating fixture has **several rows per frame**, so a
  per-row implementation fails it.
- The `does not yet split multi-row frames` error becomes unreachable for the
  partial-survivor case, and its test becomes a test that the case is handled.
- A deterministic maintenance failure is observably retried on a backoff schedule
  and quarantined after a bounded count; the counter has a **reporting half** and
  a run can fail on it (see ADR 580's decline-gate correction - a guard counter
  the engine increments and no artifact publishes reads back as zero).
- A maintenance job that would exceed the node's budget is refused with a receipt.
- Resident bytes return to baseline after a failed maintenance cycle. The exhibit
  runs several cycles and asserts no ratchet, because a single cycle cannot
  distinguish a leak from a peak.

## Guardrails

- `lsm_compaction_paused = true` is containment, not a fix, and it pauses only
  `LsmCompactionService`. The secondary-index lifecycle and the MVCC retention
  sweeper keep running, so the retention half of the loop is unaffected by it.
  **Measured over 20 minutes of containment:** resident bytes are bounded - a
  0.23-2.88 GB sawtooth against a 7.86 GB unit peak, versus the 40+ GB climb
  before - but the retention half is a **non-converging rewrite loop**. Wendelon
  read **60.3 GB and wrote 13.8 GB** in those 20 minutes (41.2 GB/hour written)
  against a **4.85 GB** store. It allocated eight slab ids, rewriting the large
  slab into a fresh ~2.35 GB file every ~3 minutes and deleting the source; the
  `backfill source slab N not found` ids are exactly the deleted sources
  (30770, 30772-30775 = `0x7832`, `0x7834`-`0x7837`, none of which exist on disk,
  while `0x7838` and `0x7839` survive at ~2.35 GB each). Containment therefore
  protects host memory and does **not** stop the work: the rewrite and the
  backfill disagree about who owns the slab, which is decision 3 above, and it is
  the retention path rather than compaction that demonstrates it.
- `MemoryMax` on the unit protects the host but converts a host OOM into a
  contained restart loop. It is not a substitute for this record.
- The heavy-churn exhibit that discharged the ADR 0055 slab-pin work did not
  produce this shape. Whatever exhibit closes this record must generate multi-row
  frames with partial survivors, or it will pass while the defect stands.

## Risks

- Frame-atomic runs may retain a frame for one surviving row, delaying reclaim of
  space the old accounting believed it was reclaiming. That is a real cost and is
  accepted here in exchange for bounded memory.
- Quarantine can hide a slab that a later fix could handle. The receipt must name
  the slab and the reason so requeueing is possible.

## Forward

Containment is applied on the local profile. The measurement that would open this
work is an attribution of resident bytes across one failed maintenance cycle -
planner accounting, retained source frames, writer clones - so the three
amplifications above are separated by measurement rather than by reading.
