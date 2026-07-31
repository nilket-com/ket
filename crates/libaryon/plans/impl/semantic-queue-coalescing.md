# Coalesce semantic analysis, then analyze tests

Scope: performance-hardening plus a definition fix, in that order — not an
ADR. The semantic reactor's unbounded FIFO computes obsolete work before
rejecting it; fix that, then enable test-aware analysis
(`CargoConfig.set_test = true`) so definitions resolve from `#[cfg(test)]`
code (e.g., jumping from a cargo warning into the test that caused it).
Deliverable is one `impl:` commit reviewed against this plan.

## Measured evidence (Codex, 2026-07-31; harness since removed — this
table is the durable record)

| file | metric | current | test-aware |
|---|---|---:|---:|
| test-heavy `host.rs` | incremental pass | 44 ms | 164 ms |
| | warm pass | 55 ms | 111 ms |
| production-heavy `runtime.rs` | incremental | 265 ms | 352 ms |
| | warm | 229 ms | 271 ms |
| polariton `editor.rs` | incremental | 241 ms | 391 ms |
| | warm | 242 ms | 343 ms |

Cold init +≈3%; RSS +14–28 MiB (<1%); definition query itself +1–4 ms;
both inactive-test definition cases flip from `NoDefinition` to correct;
span counts confirm test code became active. The blocker is not the
per-pass cost — it is the queue: `svp/semantic.rs` uses one worker and an
unbounded FIFO; every edited state is enqueued and obsolete work is
rejected only *after* RA computes it. At ~10 edits/s against ~2.6
test-aware jobs/s on `editor.rs`, one second of typing queues ~3 s of
obsolete work, and navigation waits behind all of it.

## Part 1 — coalesce the reactor queue (the third of its family)

Lineage: the minimap worker drains to newest (correct from birth); the
uast-graph worker got per-view coalescing in review; the semantic reactor
is the original instance. Apply the established pattern, strengthened for a
mixed-priority queue:

- The worker owns a persistent pending set. After the initial blocking
  `recv`, and again after **every completed item**, drain `try_recv` into that
  set before selecting the next item. Do not drain once and blindly execute a
  local batch: navigation arriving during its first analysis would then wait
  behind all remaining analyses and falsify the bound below.
- **Coalesce `Analyze` requests per buffer, retaining only the newest**
  (request-id/state stale-rejection already guarantees dropped intermediates
  were doomed at the response side; this moves the rejection before the
  computation). Preserve FIFO order among the surviving buffer slots so one
  noisy buffer cannot starve another.
- Retain exactly one newest `GotoDefinition` request globally. The session has
  one `pending_navigation` stamp, not one per view, so every older navigation
  request is doomed at response time even when it came from another pane.
  Moving that supersession before provider work prevents navigation itself
  from becoming a second unbounded queue.
- **Select navigation ahead of analysis after every drain.** A navigation
  arriving during an analysis runs before any second analysis. A navigation
  arriving in the narrow drain-to-dispatch race may wait behind the one
  analysis just selected, which is the admitted in-flight bound.
- **The honest bound, stated in code comment and here**: a navigation
  request may still wait behind one *in-flight* analysis — single worker,
  and the provider mutex serializes `AnalysisHost` access, so a second
  thread buys nothing. Post-fix worst case: ≤ one pass (~400 ms at the
  measured test-aware worst) + 1–4 ms query, versus today's unbounded
  queue. RA-level mid-pass cancellation is a possible future refinement,
  not this pair.

Tests: a controllable synthetic provider holds one analysis in flight while
the test enqueues N rapid states for one buffer, a second buffer, and
superseded navigation requests from distinct views. After release, the work log asserts: only
the newest analysis for the burst buffer executes; the other buffer executes
once; only the newest navigation for its view executes; and navigation is
selected before either pending analysis. Synchronization and a work log —
not elapsed-time assertions — prove the ordering deterministically.

## Part 2 — then enable test-aware analysis

- `CargoConfig.set_test = true` in both the sysroot-discovery and fallback
  Rust workspace loads; no configuration path silently retains the old
  behavior.
- Regression tests: definition from inside a `#[cfg(test)]` module to a
  production symbol, and to a test-local symbol, both resolve (the two
  measured flip cases); diagnostics/highlights in test modules become
  active (span-count assertion).
- The measured costs above are the accepted price; the coalesced queue is
  what makes them affordable. Part 2 does not land before Part 1 in the
  same commit's history of checks.

## Non-goals

- A second analysis worker (mutex-serialized; would add nothing).
- RA snapshot cancellation mid-pass (future refinement if the ≤-one-pass
  bound proves insufficient in the diary).
- `:dec`/declaration navigation (unrelated; recorded as later).
- Any change to stale-response rejection semantics (they stay as the
  second line of defense).

## Review checklist

- [ ] Persistent per-buffer newest-only coalescing; cross-buffer FIFO
      fairness kept.
- [ ] Global newest-only navigation coalescing, matching the session's single
      pending stamp; navigation selected ahead of pending analyses after every
      drain; the ≤-one-in-flight bound documented where the code enforces it.
- [ ] Burst test proves obsolete states are never computed.
- [ ] `set_test` lands after coalescing with both regression cases green.
- [ ] Existing semantic/graft/navigation suites unchanged and green;
      the measured table's correctness flips reproduced.
