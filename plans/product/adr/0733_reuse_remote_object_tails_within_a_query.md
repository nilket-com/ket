# ADR 0733: reuse remote object tails within a query

Status: accepted for local RED-first implementation, 2026-11-14. Not delivered; the plan stays off main until its reviewed implementation follows. Defect DC-223, which this narrows and does not close.

## Why
- In the ParentDecisions mode a table query reads each selected record twice, once to prove it (`prove_rows` → `stored_rows_at_version` → the slab manager's `read`) and once to execute, and before the first execution read of a slab it may load that slab's statistics block. When the record is in a remote object, each of these asks the store for the object's size (HEAD) and its tail window, one request after another; the statistics load then asks for a range that lies inside the tail it just fetched. Nothing in a query remembers bytes it already fetched: the proof decodes a record, takes a digest and drops everything.
- Read from source, under these conditions: an encrypted object that fits the tail window, one selected record per object, a statistics block that applies, no extra index range, no repair, no retry inside a request, no promotion fetch. Then per object: proof 2 requests, statistics 3 (only while the node's parsed-statistics cache does not hold that slab), execution 2. For N such objects that is 7N requests on a first query and 4N with parsed statistics already cached, of which 2N bring bytes the query does not already hold. These are a source model for a controlled case, not a measurement of any run and not a count that holds in general: a later query can trigger promotion (a whole-object fetch after the slab's third remote read) and the parsed-statistics cache can remove the statistics load altogether.
- Remote requests are the unit a slow or distant store charges for. The required three-node object-store workflow fails today on query time; this ADR removes redundant requests and attributes no particular past timeout to them.

## Decision
1. A query-scoped holder of fetched tails, `QueryTails`, private to the storage manager. It maps an object URI to the evidence of ONE fetched tail: the object's size, the tail's start offset, and the tail's bytes exactly as fetched (`start + len == size`).
2. `parent_decisions_table_query` creates one holder from its own slab manager and passes it, as an optional argument, down BOTH paths that read remote records for it:
   - proof: `prove_rows` → `proved_location` → `stored_rows_at_version` → the manager's `read` → `read_with_reference_origin` → `read_expected_version_once` → `read_from_l2` → `read_encrypted_record_and_header_from_l2`;
   - execution over the proved rows: the planner's per-location record read (`slab.read(location)`) down the same chain, and the per-slab statistics check (`table_stats_may_match_location` → `load_tiered_column_stats_block`).
   Each function on those chains gains an optional reference and passes it on; every other caller passes none and is unchanged byte for byte (Today mode, the sweeper, repair, recovery, promotion, tests of those paths).
3. At the two functions that fetch a size and a tail (`read_encrypted_record_and_header_from_l2`, `load_tiered_column_stats_block`): with a holder, look the URI up. On a hit, use the held size and tail and make no HEAD and no tail request. On a miss, fetch as today and offer the result to the holder. After that point the function does exactly what it does today with those bytes.
4. The statistics containment shortcut is limited to a valid query-local holder entry, already held or newly admitted. It takes the statistics range from those tail bytes only when ADR 0730's `covered_by_tail` rule proves complete exact-length containment; otherwise it fetches the range as today. With no holder, a mismatched owner, or failed admission, the existing statistics fetch path remains unchanged. This ADR does not introduce a global statistics optimization for other callers.
5. A hit supplies BYTES. It supplies no authorization, no prepared key, no plan and no decoded result: every read still resolves its current pointer and generation before it knows its URI, builds its own plan (the root-binding, tenant-key and slab-key checks), verifies the footer-and-index authentication code, checks the index, takes ciphertext only under the containment rule, decrypts with AEAD, and checks the record header and the frame, as now.

## The holder (API, accounting, lifetime)
```
pub(crate) struct QueryTails { owner: usize, tail_window_bytes: u64, state: Mutex<State> }   // owner: identity of the creating SlabManager's store
struct State { entries: HashMap<Box<str>, HeldTail>, charged: usize }
#[derive(Clone)] pub(crate) struct HeldTail { size: u64, start: u64, bytes: Bytes }   // start + bytes.len() == size

impl SlabManager { pub(crate) fn query_tails(&self) -> Arc<QueryTails> }
impl QueryTails {
    fn get(&self, owner: usize, uri: &str) -> Option<HeldTail>;                         // a clone of the handle; lock held for the lookup only
    fn offer(&self, owner: usize, uri: &str, size: u64, start: u64, fetched: &Bytes) -> bool;
}
```
- Namespace: the holder remembers which manager's object store created it and captures that manager's fixed tail-window bound. `get` and `offer` take the caller's own identity and do nothing for any other; a URI is a key only inside one query and one store, never a general cache key. The implementation must identify the actual stable owner token and show why it cannot be confused by a move or address reuse during the holder's lifetime; a bare address of a movable value is not a sufficient argument.
- Admission (`offer`): refused, with no change, when the tail is empty, when `start + len != size`, when `len` exceeds the manager's tail window, when the URI is already held, when the entry count is at its limit, or when the charge would pass the budget. No eviction: once the budget is reached, later reads simply fetch as today.
- Logical admission charge: checked `len + uri.len() + 160` bytes per entry, including a bookkeeping allowance. Limits: 8 MiB charged and 4,096 entries per query, constants beside the existing per-query decoded-record cache (8 records or 32 MiB). This is not a proved all-in allocator or RSS limit: actual map capacity, allocation headers and rounding are separate bounded overhead, with container growth bounded by the entry cap. The map is created with no reserved capacity and cannot grow past the entry limit. Admission arithmetic and extent arithmetic fail closed on overflow.
- Detached copy: `offer` stores `Bytes::copy_from_slice(fetched)`, a detached `len`-byte payload, so a held tail never pins a larger backing buffer of the transport. This does not claim physical allocations avoid allocator rounding. Transient cost: for the rest of that one read both the fetched buffer and the copy exist; the added copy is at most one admitted tail window. The holder does not bound pre-existing fetch/decode allocations.
- No lock across an await: `get` and `offer` are synchronous; the fetch happens between them with no lock held. Two reads of one URI cannot race inside a query today (the reads are sequential); if they ever did, the second `offer` is refused as a duplicate and both results are identical bytes.
- Lifetime: the `Arc<QueryTails>` lives in `parent_decisions_table_query`'s own frame and is lent by reference to the proof and to the execution it awaits. It is dropped when that function returns or its future is dropped: on the answer, on an error, on cancellation and on the caller deadline alike. No stream, writer task or background worker receives it; the implementation states where it verified that, and a test holds a `Weak` to show it is gone after a completed query and after a cancelled one.
- Additive memory: the logical charge is bounded by its budget; retained payload/key bytes and container bookkeeping are bounded by that budget and the entry cap per RUNNING ParentDecisions query. Exact physical RSS is not asserted. Section 8's memory requirement remains failed and unwaived; this adds bounded retention per query and is not offered as part of its solution.

## What does not change
The object and pointer formats; replicated state; the object-store interface; the tail window; the remaining HEAD (the size is not in replicated state); whole-object reads: promotion and its payload-hash validation fetch for themselves and are never served from the holder; the per-slab count of remote reads that triggers promotion, which goes on counting READS exactly as now; the node's parsed-statistics cache and its own hit path (a hit there already skips the load; nothing about it is claimed or broadened here); the query no-progress bound, its units, its default and the caller deadline (a unit that needs no request completes at once); authority checks between phases; the NotFound re-resolve path; retries inside a request; request concurrency (none added); every path other than a ParentDecisions table query.

## Limits, stated
- The model's 2N holds for the controlled case only. Objects larger than the tail window still fetch what lies outside it; a slab with several selected records still decodes each from the one held tail; a promotion fetch, a repair, a retry or an index range adds requests as today.
- 2N sequential requests are still paid. Making them concurrent, or removing the HEAD, is not decided here.
- No query time is promised on any network, and no earlier failure is attributed.
- A query larger than the budget gets the reuse only for what was admitted first.

## Acceptance
Controlled counting fixture (stated so the counts mean what they say): the existing scripted object store that logs every HEAD, range and whole-object request; encrypted objects smaller than the tail window; one selected record per object; statistics applicable; promotion disabled IN THE FIXTURE ONLY (the fixture's existing setting; the real workflow's policy is untouched); every request of the query path counted, statistics included.
1. RED first, on unchanged product code: one ParentDecisions query over N proved rows, through proof and execution, asserts the target 2N requests and unchanged answers and fails for the redundant requests; record the observed 7N cold / 4N warm source-model counts in that controlled fixture. After the change: 2N in both, the same answer rows and the same proof result. A test merely asserting today's counts is a baseline, not the RED.
2. The statistics range comes from the tail when wholly contained, and is fetched when not.
3. Inconsistent tail shape is rejected at admission and follows the ordinary uncached path; it need not make the query fail when a subsequent valid fetch succeeds. Shape-consistent bytes with an altered footer or flipped ciphertext are refused by the existing read/crypto validation paths on reuse (the ADR 0730 controls driven through a held tail). Separately, a store whose answer changes after one valid fetch is shown NOT to be consulted again for that URI within the query (that is the reuse, not a corruption test).
4. Budget: with limits smaller than the workload the answer is the same and the reads beyond the limit make today's requests; an empty, oversized, inconsistent or duplicate offer changes nothing; the charge never exceeds the limit.
5. A slab republished under a new URI between the proof and the execution misses and is fetched.
6. The tenant's key removed between the proof and the execution: the execution's record read fails as it does today, although its bytes are held.
7. Namespace: a holder from another manager is ignored.
8. Lifetime: the holder is gone after a completed query and after a cancelled one.
9. Every other caller makes the same requests as before: the ADR 0730 tests and the stall-bound tests pass unchanged, and the storage-manager fixtures that count ranged and whole fetches pass unchanged.
Checks: focused selection from the touched call graph: `network::data_server` (reader, the covered-tail and stall-bound modules), `storage::manager` (tiered I/O, statistics, version identity, the LSM driver's read path) with their test modules, and the ParentDecisions in-process cluster tests that read tiered rows; then ONE whole-crate run (a read-path change; ADR 0728's omission is not repeated); then ONE execution of the real one-host object-store workflow at its declared 2000 ms profile, the general default unchanged, reported whatever it does (it fails today on query time; a pass or a failure is one sample).
Cost: the change and its tests, a few hundred lines; build about 8 minutes; focused tests minutes; whole crate about 13 minutes; the workflow 3 to 6 minutes.

## Stop points
Plan review → RED (1) on unchanged code → implementation → source review → the whole-crate run and the one real workflow → delivery ruling.
