# ADR 0128 - A holder's verification reuses decoded frames within one call, under a nonblocking allowance

## Status
Proposed (local), 2026-11-24, revision 5: the plan of a **local verifier-only implementation candidate**. Not a delivery decision. Revision 1 (`5ea90d101`, "reading each stored frame once") is withdrawn: its ordering premise failed.
- **Per Codex:**
  - 01a20c93 and its acceptance: the discriminator;
  - 01a20cf2: the baseline accepted; the ordering premise to be checked first;
  - 01a20d06: the ordering check accepted (3,031 frame runs for 4 frames); option 2 selected for a bounded offline design and test-only evaluation, verifier first; implementation clearance for revision 1 withdrawn.
  - 01a20d2b: the frame-set evaluation accepted; GO for a local verifier-only candidate with **four entries per verification and an experimental 8 MiB (8,388,608 B) aggregate allowance per node across all verifier callers**: a candidate sizing, not headroom shown on the 1 GiB node, not a production default.
  - 01a20dc5: the exact-tree lanes and the release evidence accepted, delivery held; a local correction of the accounting (R1), the semantic oracle (R2) and the read observation (R3). Four entries and 8 MiB kept.
  - 01a20e68: the 26 base and candidate semantic outcomes and the scoped read counts accepted, delivery held; a narrow local correction: a kept frame names its store without owning it; a hit whose index cannot be charged leaves the set on every outcome; the buffers' owner records are bounded in the cost; the uncharged holder does not price its batch.
- **Base:** origin/main `1d43cf0ff` (the delivered `fd6bd7546` and two documents).
- **Evidence** (a real one-voter ParentDecisions store, AppendOnly MultiRow, a 16-column approximation of lineitem, release; DC290-CHILD-DISCRIMINATOR.md, ADR0128-ORDERING-CHECK.md, ADR0128-FRAME-SET-EVALUATION.md):
  - a child of 4,096 rows is 4,096 copies in **4 frames**, visited by the verifier in an order unrelated to the frames: **3,031 consecutive runs**;
  - today the live call reads and decodes one frame per copy: **4,096 reads**;
  - a walk in the manifest's own order keeping decoded frames: 1 frame kept gives 3,087 reads; 2 give 2,067; **4 give exactly 4 reads** (4,092 hits, no eviction), 18 ms against 1,122 ms;
  - the evaluation priced one decoded frame at 1,605,235 B (130,696 B of frame buffer and 1,474,539 B of Arrow memory by `get_array_memory_size`), about 12 times its stored bytes, and four kept frames at 6,356,694 B. **That Arrow figure overcounts** (found under 01a20dc5): a decoded stream keeps ONE body allocation that every column buffer points into, and the per-buffer sum counts it once per buffer. Counted once, by ownership (D2), the same dense verification peaks at **1,812,012 B** for its four frames, about 3.5 times their stored bytes.
- **Not claimed:** NEXT6's four failed writes and its periodic excess are not attributed. Timings are a fixture's, not NEXT6's.
- **Held:** product work, Section 8 runs, the campaign, qualification and delivery.

## Context
- `verify_owned_rows_waiting` (`network/data_server/child_writes.rs`) walks a child's manifest in order. Per version: the owner lookup; the version's same-HLC locations under its own admission; for each location a frame read, a **whole-frame decode** and a one-row slice (`rows_at_locations`); then each copy's digest.
- A MultiRow frame holds many versions, and the manifest's order (by owned version) is unrelated to the packer's (routing locality, then identity). So consecutive versions are usually in different frames, and one retained frame saves about a quarter of the reads.
- The whole saving needs **every frame of the child** to stay decoded for the call: with k of F frames kept the reads fall only to about (1 − k/F) of today's.
- That is memory: a kept frame retains its stored bytes and its decoded body, about 3.5 times its stored size in the fixture.
- The verifier is shared: the holder's own child (with ADR 0126's wait), a replica's child, repair, startup validation and replacement recovery.

## Decision
### Scope
- **Changed:** within one `verify_owned_rows_waiting` call, a physical frame already decoded for an earlier copy may serve a later copy without a second read and decode.
- **Not changed:** the manifest walk and its order; no prelisting and no reordered I/O; the owner check; each version's admission and ADR 0126's wait and deadline; each copy's own resolution, pin and per-backing verification; the every-copy digest; tombstones; every refusal and its precedence; the pre-append missing-copy scan (left as delivered); packing, identity, wire and durable state.

### D1. A per-call set of decoded frames
- The delivered `FrameReuse` (one held frame) becomes a small set. A frame serves a copy only when that copy's own `resolve_read_target` gives the **same backing generation** (the same local store, the same rehydrated store and tier generation, or the same L2 pointer) and the same slab, offset and length. Otherwise the copy is read as today.
  - A kept frame names a local or rehydrated store by a **weak pointer**: it owns nothing of the store, so a store the node has released (a slab tiered while a frame from its file is held) drops its index, file and heap at once. While the weak pointer lives the store's address cannot be given to another, so the identity cannot be reused. The one-frame holder names its store the same way.
- On a hit, a tiered, rehydrated or L2 copy is still verified against its own target; the frame's index entries are parsed once per held frame.
- The set lives for one verifier call. Nothing is kept between calls, attempts or sites.

### D2. Two limits, and what they are derived from
- **Entries per call: 4** (`VERIFIER_FRAME_ENTRIES`). Four attained the same result as eight in the evaluation, and a child had 4 frames in every shape measured.
- **Bytes: a node-wide, nonblocking allowance of 8 MiB** (`VERIFIER_FRAME_ALLOWANCE_BYTES`), an internal constant with a test-only override. No public configuration and no new production trace. Simultaneously active verifications have no useful bound: a tenant may have 8 writes in flight (`DEFAULT_TENANT_MAX_INFLIGHT_WRITES`), tenants are unbounded, and repair, startup validation and replacement recovery verify too. So the bound is aggregate:
  - one atomic byte counter for the node;
  - a frame is kept only after a **try-reserve** of its cost succeeds; the reservation is released when the frame leaves the set;
  - **cost** (`FrameCharge`, requested bytes; an allocator's own rounding is not counted) is, part by part:
    - the frame's own record, one allocation per kept frame. The set's slots are inline in the call's value, so a set holding nothing retains no heap, and there is no spine whose capacity could outlive its charge;
    - the stored frame's buffer, at capacity;
    - the decoded batch by its actual ownership (`batch_retained_bytes`): each distinct allocation behind its arrays' buffers once, at capacity, plus 96 B for its owner record (a bound: Arrow does not expose that record; it is two counts, a pointer, a length, how the allocation is freed and an optional pool reservation); each column's array record and shared-pointer counts; the column list at `max(4, 2 * columns)` slots (the most a pushed list holds); the schema's record, fields (names, types, metadata) and metadata, at capacity;
    - the parsed index entries when they are parsed, at capacity (reserved then, or the frame leaves the set before the call returns, whether the copy is served or refused). Entries own no heap;
    - what the backing reference keeps allocated: an object pointer's or a rehydrated generation's URI, at capacity, and for a local or rehydrated store the store's emptied record and counts, which a weak pointer holds until it goes. The store itself is the node's and is never the frame's.
  - **release order:** a frame is freed first and its reservation returned after, on eviction and at the set's end, so credit is never available to another call while the memory it paid for is still held by the set.
- **The size is a candidate.** One dense verification of the fixture charges 1,812,012 B, so four in flight (7,239,725 B) fit together; a fifth would contend. The real lineitem's decoded frame cost is unmeasured; the allowance is not borrowed from the tier-history budget.
- **No eager per-call slots:** a set starts empty and reserves per frame; calls that keep nothing charge nothing.
- **The one-frame holder keeps its behaviour:** F1's certified-content reads and the missing-copy scan keep one uncharged frame and their existing bound, and do not price their batch. Their frame names its store weakly too, so it no longer extends a released store's life.

### D3. Pressure never costs a write
- A frame too large for the allowance, a failed reservation, or a full set whose least recently used frame cannot be released: **the copy is read, decoded, used and dropped, exactly as today.**
- Nothing waits for memory. Nothing is refused for memory. ADR 0126's retirement deadline is not extended.

### D4. What reuse adds to memory, and what it does not
- **Baseline today:** each copy's read holds one decoded frame until its version's rows are digested (the returned one-row slices share the frame's buffers).
- **Added by reuse:** the frames in the set, bounded by D2.
- **Not added:** slice lifetime. The verifier still collects one version's rows and digests them as today, so the precedence between an earlier digest mismatch and a later read error is unchanged. A slice of a frame evicted mid-version keeps that frame's buffers until the version ends, as a slice does today; that is baseline working memory, at most one version's copies.
- **The allowance is not a whole-node peak.** It bounds what the sets keep between copies. Outside it, as today and per call in flight: the read in progress (the frame being read, its index parse and its decode, including a decoder's temporary buffers), and one version's returned slices. A frame that is read and not kept is that baseline exactly.
- **No small-node bound follows from a single holder's counter.** The bound is the allowance plus the baseline of every call in flight.

### D5. Errors and refusals
- Unchanged: `Unverified(error)` for a failed read; "a manifest version is not stored" for an empty answer; "a stored copy differs from the manifest" for a digest mismatch on that copy; the same error kind for a row index outside its frame.

## Rationale
- **Option 2 over reordering:** the verifier's refusals and admissions are tied to the manifest walk. Keeping the walk and only skipping repeated decodes leaves every semantic on its current path.
- **Aggregate, nonblocking:** with no bound on simultaneous verifications a per-call cap alone bounds nothing, and a blocking budget would let memory pressure delay writes.
- **Honest about the cliff:** below a child's frame count the saving is proportional, so an allowance too small for one child's frames buys little.

## Controls
### The semantic oracle (the base, then the candidate; `child_writes/verifier_semantics/tests.rs`)
Actual verifier calls on a stored child of 48 rows in 12 frames, with a test-only pause between two versions of one call and a test-only index seam that lists one more location for a version (the read path is untouched). Outcomes are asserted; they are the same on both paths.
- the stable child verifies;
- **a second copy with the same content** is checked and verifies; **a second copy with other content cannot be stored:** the store's conflict check refuses the append (`ConflictingVersion`);
- **a listed second copy with other content** (another row's location, in the same frame and in another, listed first and last, at three manifest positions) refuses the child, "a stored copy differs from the manifest";
- **a read error and a digest mismatch in one version:** a manifest digest no copy matches refuses as a differing copy; with an unreadable second location listed (first or last, with or without the mismatch) the read's refusal is the one reported ("slab record magic mismatch"), because every copy of a version is read before any is compared;
- a version stored with other bytes before its child refuses the child, "a stored copy differs from the manifest", with the intent left `Intent`;
- **a fence raised between versions** refuses at the next version's own admission ("fenced"), with no wait;
- **with a holder's wait** the fence is waited only to the inherited absolute deadline and then refused; cleared inside the deadline, the call verifies;
- the rows' slab **sealed between versions** (the same local backing) leaves the call verifying.

### The candidate only
- **Accounting, at the store** (`storage/manager/verifier_frames/tests.rs`), on charged sets with their own allowance:
  - a batch's retained bytes count a shared allocation once and separate allocations each, and grow with a field's name and the schema's metadata;
  - a kept local frame's charge is the sum of its parts, each against a cost known apart from the charge: the stored frame's length, the columns' data bytes, an independent decode's retained bytes, no index, and for the backing only the weak pointer's record;
  - after every step the allowance equals the sum of the held frames' charges: a kept frame, hits, an eviction of the least recently used frame, a frame not kept for want of room (another call's frames are never evicted), a frame larger than the whole allowance, and the set's end (zero);
  - **a local backing that becomes tiered** (the same store, now with a tier pointer): the next copy is a hit and is verified against its own version; the index is parsed then and the charge grows by exactly the index; a copy naming another version is refused from the held frame with the ordinary read's refusal;
  - **a hit whose index cannot be charged** (room for the frame and not one byte more): a served copy and a copy naming another version both leave the set empty and the allowance at zero before the call returns, inspected with the set still alive;
  - **a changed backing generation at the same coordinates** (the local file, then the object): the same copy is read afresh, never served by the held frame, and returns the same row as before and as the ordinary read; the new frame serves its own copies, each verified; its charge includes its URI and follows the URI's length. With the set still alive and still holding the local frame, the local store it named is gone once the slab is tiered.
- **Charge through the verifier:** a refusal between versions returns the call's charge; a cancelled caller's still-running work keeps its charge until it ends, then returns it (`child_writes/verifier_charge/tests.rs`).
- **Through the holder's entry point** (`verifier_frames_tests.rs`, one-voter store). Reads are each verification call's own physical reads and hits, recorded per store by a test-only observation, never the process's read histogram:
  - a dense child's live write is one call of 4 reads and 4,092 hits, and leaves nothing charged;
  - four writes in flight, at the default allowance and at 2 MiB (where they contend): all commit, each call examines each of its 4,096 copies, the peak stays under the allowance and nothing stays charged. Per-call counts are reported and bounded; they depend on interleaving and are not fixed values;
  - with no allowance or one below a frame the write commits through a read a copy, with no hit and nothing charged.

### Not exercised
- A rehydrated backing (the object and the local file are).
- The real lineitem's frame and decoded sizes.
- An allocator's rounding and bookkeeping, which the charge does not count.

## Consequences (The "Negatives")
- New node-wide state (one counter) and a tunable that must be sized against the small node.
- A cliff: partial coverage of a child's frames gives a partial saving.
- A kept frame retains about 3.5 times its stored size in the fixture; this spends memory to save CPU.
- The charge walks a kept batch's buffers once when the frame is kept.
- A baseline cost only; nothing here is shown to change NEXT6's periodic excess or its failures.

## Open Questions
- The allowance's size on a ~1 GiB node, and the real lineitem's decoded frame cost.
- Whether a child's frame count is always the fixture's 4 for the default table spread.
- Whether it pays off under Section 8 at all: a measured question for a later, separately cleared diagnostic.
