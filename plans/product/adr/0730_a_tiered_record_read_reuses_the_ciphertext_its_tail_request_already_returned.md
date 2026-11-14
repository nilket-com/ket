# ADR 0730: a tiered record read reuses the ciphertext its tail request already returned

Status: accepted as a design, 2026-11-14 (Claude, Codex 01a1d779). Defect DC-223, which this narrows and does not close. Records: `reviews/dc223-20261113/`.

## Why
- Reading one record of an encrypted tiered slab from the object store is a sequence of requests, each awaited before the next: the object's size (HEAD); its tail window, `size - tail_read_window_bytes .. size` (64 KiB by default, so an object no larger than the window is fetched whole); an index range when the index is not in the tail; then the record's ciphertext range.
- In a query's execution that whole sequence is one awaited unit under the node's no-progress bound, 250 ms by default (ADR 541, ADR 0719). Requests that are each under the bound can sum past it.
- Seen in a real daemon, direct LAN, default bound (run pd-offload-20261114T002030Z, instrumented): a record read's HEAD completed after 225 ms, its tail request had been pending 25 ms when the read was dropped at 251 ms, and the query ended QUERY_STALLED. That read never received its tail and made no third request. In the same run every object was 8.9 KiB, and in the reads that did complete the tail request returned the whole object and the third request then asked again for a range inside it: bytes the read already held.

## Decision
- After the read has planned the record's ciphertext range, it takes those bytes from the tail window when, and only when, (1) the tail response has exactly the requested length, `size - tail_start`, and (2) the whole ciphertext range lies inside `tail_start .. size`. It then makes no ciphertext request. In every other case it requests the range as before.
- The bytes go through the same `decrypt` of the same plan, then the same tenant and header checks. Nothing is trusted that was not before: the size still comes from HEAD, the footer and index from the fetched tail (and index range), every offset from the plan. A tail response of any other length is never mapped to object offsets. Offsets are computed with checked arithmetic and the slice is bounds-checked.
- Within one read this uses one fetched copy of the object's bytes where two requests were made before.

## What does not change
HEAD; the tail and index requests; the plan; authentication; every object format; the object-store interface; the stall bound, its meaning and its default; the caller's deadline; retries; rehydration; the column-statistics loader, which has the same shape and is left alone here. No cache across reads.

## Requests for one record read
| Object and record | before | after |
|---|---|---|
| encrypted, no larger than the tail window, index in the tail | 3 | 2 |
| encrypted, larger, the record's chunk run wholly inside the tail (a short last chunk) | 3 | 2 |
| encrypted, larger, chunk run not wholly inside the tail | 3 | 3 |
| encrypted, index not in the tail | 4 | 4, or 3 when the chunk run is inside the tail |
| unencrypted | 1 whole-object request | 1 |

## Limits, stated
- One fewer serial request for covered ranges, and nothing more is claimed. Any single request can exceed any finite bound. Whether a real store then serves these reads within the default bound, and how often, is not known; in the run above the read's HEAD alone took 225 of the 250 ms.
- The earlier failures recorded under DC-223 are not attributed by this: the two real-daemon runs through a relay, and the post-restart stall of run pd-offload-20261113T235743Z.
- HEAD remains because the size is needed first. Removing it would need either a suffix request that also returns the object's size, across every store implementation, or the size recorded in the replicated tier pointer. Neither is decided here. Nor is any bound for remote stores.
- A query in the three-node mode also fetches each slab's statistics block (three requests) before each proved read, with no predicate to use it. Registered; not changed here.

## Acceptance
1. Counting store, an object smaller than the tail window: each record read is exactly HEAD and the tail; its bytes are the appended frame; its header equals the header the unchanged path returns for the same rows where the tail window does not hold the record (four requests there).
2. A larger object with a short last chunk, default window: a record whose chunk run is inside the tail is read in two requests; every other makes the ciphertext request, for a range not inside the tail; both occur; every read returns its appended frame.
3. A tail one byte shorter than asked is not reused (the read is served only with the ciphertext request); one byte longer is refused by the existing footer check; a footer cut or altered is refused; a ciphertext byte altered inside a reused tail is refused, with no further request.
4. The mapping from object offsets to the window, alone: covered only by a window of the requested length that holds the whole non-empty range.
5. Delaying store, 100 ms per request on a paused clock, default bound, an object smaller than the window: the query is served, two record reads of two requests each. Without the reuse the same query ends QUERY_STALLED with the third request outstanding.
6. The existing characterization (a 272-byte tail window, four requests per read, stalled at 100 ms a request) is unchanged and passes.
7. One real three-daemon execution of the offload and restart test on this product code, its result recorded whatever it is.

## Validation record
Development evidence on bigfoot, `~/work/reviews-logs/dc224-dev/`, debug builds, one execution each, working tree before the commit.
- e2 (first run of the new tests with the change): 10 run, **3 FAILED**, all three defects of the tests: a control that denied the reuse by lengthening the tail answer met the existing footer check instead (the control is now the unchanged path in a store with a 272-byte window, and a tail one byte SHORTER, which parses and is served only with the ciphertext request); the larger object had no record inside its tail (its chunks are 64 KiB and its last one was full; a hundred rows now leave a short last chunk).
- e5 (0e378fd2…): the five new tests and the five existing stall-bound tests, 10 of 10.
- e6red (1963465b…) and e7red (75449833…), the same tests with the reuse disabled in the product: the four new behavioural tests FAIL (three requests where two are required; the short last chunk's range requested though the tail holds it; and, in e7red, the delayed query ends QUERY_STALLED with `[Head, Range(0, 8809), Range(72, 8280)]` asked), the mapping test and the existing five pass.
- e8 (55801d82…): those ten with the crate's tiered, object-store, range-read and crypto unit tests, 143 of 143. `cargo fmt --check` clean.
- No existing test's expectation was changed. One fixture function gained a parameter (the tail window), its existing caller unchanged in effect.
- Acceptance 7, the real three-daemon offload and restart test on this product code (harness commit 38d710b9c above the pair, uninstrumented, run pd-offload-20261114T005124Z, seal 34920e44…): **FAILED**. 64 rows acknowledged, all three nodes offloaded, and the judged query before any restart ended QUERY_STALLED after 15.6 s. Which wait expired is not established (a record read is still two requests; the statistics load before it is still three). This change does not make that scenario pass at the default bound. The scenario stays failed and its test is not landed with this pair.
- The same scenario before this change, for the record, not as a comparison: queries answered in runs 231914Z and 235743Z (its first), stalled in 235743Z (its second, after restarts) and 002030Z. Two answers and three stalls in five judged queries over four executions, on different builds and placements: not a rate.
- Whole crate on this product code: a first attempt (final3, f7085c9e…) did not build (the build directory's file system was full at link time; no test ran). With the build directory on disk and test files on tmpfs as usual, final4 (8ef21a01…): 4652 run, 4652 passed, 22 skipped, both three-node workflow variants and the 2 MiB worker-stack test among them. One run.
- Status of this pair: its focused tests and one whole-crate run pass; its real three-daemon scenario FAILED and stays failed. Delivered as a scoped reduction of requests, not as remote tiering shown to work.
- Not run: a release build; three hosts; any store other than the development RustFS; any measurement of how often a read stalls.
