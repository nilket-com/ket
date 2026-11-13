# ADR 0727: a table write is served within a daemon's worker stack

Status: accepted, 2026-11-13 (Claude, Codex). Defect DC-232, found by ADR 0726's real-process run; attribution and fix boundary agreed by Codex 01a1d434, product source cleared by Codex 01a1d442 (`reviews/dc232-20261113/`, `reviews/dc230-custody-20261113/`). **Its focused selection FAILED 673 of 674 (acceptance item 3 is unmet; see the validation record).**

## Why
- On main (ADR 0725, 900b573f0) the forming node of a real three-node ParentDecisions cluster aborts about 3.5 s after start: "thread 'tokio-rt-worker' has overflowed its stack". The cluster never serves its first table write.
- The real three-daemon test, unchanged launcher, debug build, one execution each: ADR 0724 (cba1bc04b) passed; ADR 0725 (900b573f0) failed. So ADR 0725 introduced it. ADR 0725 was delivered without running that test; its validation record does not say so and cannot be edited, so this ADR records it.
- It is not unbounded recursion. With each daemon under a debugger, the overflowing worker is serving the table write: 96 frames, 2,095,544 bytes, the whole 2 MiB worker stack. Ten poll frames of nested async functions, each holding its child future by value, account for about 1.97 MiB: the request dispatch block (444,768 bytes), `serve_table_write` (295,920), `table_write_answering` (240,944), `serve_table_write_stream` (198,784), the observed-work block (195,472), the task-local scope (192,848), `coordinate_table_write` (109,632), `handle_table_write` (104,576), the task-local closure (96,704) and `coordinate` (92,576).
- No in-process test could show it: the test wrapper gives every thread a 64 MiB minimum stack, a runtime built in a test inherits it for its workers, and a daemon's workers have the default 2 MiB.

## Decision
1. **Hold the table write's child futures on the heap at six boundaries**, in place: the dispatch arm for `TABLE_WRITE_REQ`; the write future inside `serve_table_write_stream`; `handle_table_write` from `serve_table_write`; `table_write_answering` from `handle_table_write`; `coordinate_table_write` from `table_write_answering`; and `coordinate` under the deadline wrapper. Each is the same future, polled by the same caller to the same end.
2. **Nothing else changes.** Request ownership, admission, the task-local work token, the early answer joined with (never raced against) the remainder, cancellation, shutdown lifetimes and every timeout are as they were. Nothing is spawned or detached. No format, frame or behaviour changes.
3. **The daemon's worker stack is not raised.** That would hide the growth.
4. **A regression test in the ordinary lane**: `table_writes_are_served_within_a_daemons_worker_stack` forms the three-node workflow cluster and serves eight table writes, each a committed acknowledgement, then an exact read-back from every node, on a runtime whose workers are set to 2 MiB explicitly (which overrides the inherited minimum). The fixture's own futures stay on the test thread. Every request goes through the client, the transport and each node's connection task.
5. **The real-process test is a required, recorded run** for changes to the daemon's runtime, startup, transport or request path. `ci/README.md` says when and why, and `AGENTS.md` points to it. This is a written delivery requirement that a person or agent must follow; nothing selects or runs that test automatically. The regression test of item 4 is the automatic coverage.

## Algebra
None: no key, identity or ordering changes.

## What this does not do
- It does not measure a release build, any other request path, or which ADR 0725 edit grew which frame.
- It does not shrink the futures themselves; it stops six frames from holding them by value. The margin is stated below, not designed.
- DC-224, DC-223, DC-230 and DC-231 are unchanged.

## Acceptance
1. The regression test aborts with a worker stack overflow on exact main with only the test added, and passes with the fix.
2. The real three-daemon test passes once on the exact tree to be delivered.
3. The focused selection around the write path passes: the three-node workflow binary, the ParentDecisions cluster tests, the data server's tests and the architecture tests.

## Validation record
Development evidence on bigfoot, `~/work/reviews-logs/adr0727-dev/`, working tree before the commit.
- red1: exact main plus the test only: the test aborted, "thread 'tokio-rt-worker' has overflowed its stack" (SIGABRT), in 3.0 s.
- green1: with the six boundaries boxed: passed, 8.0 s.
- Margin probes, with the test's stack constant changed for the probe only and restored: passed at 1.5 MiB and at 1.25 MiB; aborted at 1 MiB and at 512 KiB. Those are four single observations. The site of the 1 MiB abort was not inspected, so they establish no minimum for the table-write path, and one pass at 1.25 MiB does not measure every execution's headroom. The six boundaries were not reduced to a minimal set.
- f1, focused selection (three-node workflow binary, ParentDecisions cluster tests, data server tests, both architecture binaries, client data API, data ring wire): 673 of 674 passed, **1 FAILED**: `certified_tier_cleanup_rewrites_and_retires_the_leaders_tiered_slabs` (one retained object where two were expected, `parent_decisions_cluster_tests.rs:4325`). The same test failed the same way in ADR 0717's checkpoint and has never been attributed; it passed in ADR 0726's focused run earlier the same day. This is DC-222's signature. By source the test's accounting has no ordering guarantee from the conditions it waits on, and boxing can change scheduling, so it is not claimed independent of this change or harmless. It was not rerun. **Acceptance item 3 is unmet.**
- After review (Codex 01a1d442): the test now closes every member in order before it ends, and a stray `mut` is gone. green2: the corrected test, once, at 2 MiB: passed, 8.0 s. The focused selection was not rerun for this test-only change.
- **The real three-daemon test PASSED, once,** on impl dbf7dfbd06dc58ed16d73134b38ae8c5ced28131 (tree efd412625e67f03bb359cbbdfbc1542ff380bcd5), unchanged launcher and daemon stack, debug build: bigfoot `~/work/reviews-logs/pd-smoke-20261113T093610Z`, seal cdf52f02cf2c546d8795cb12962a15d00863c1ecd450423373f4bc72f1775e65. The delivered tree differs from that one only in this record's text.
- Not run: a shared full checkpoint (agreed unnecessary for this change, Codex 01a1d442); a release build.
