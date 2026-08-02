# Fan out Rune relation views

Scope: follow-up pair to the closed first-contact implementation (85fd3d10),
forced by measurement #2's immediate dogfood evidence — one relation must
feed a table and a plot in one execution. Product-side only: envelope,
scheduler, and sink fan-out. No protocol change, no plot-grammar change, no
new relation semantics. Deliverable is one `impl:` commit reviewed against
this plan, plus corpus v0.4.

The architectural prize is stronger than convenience: every view in the set
renders the **same generation-stamped bounded result**, so disagreement
between a table and its chart becomes unrepresentable — two presentations of
one stamped execution, never two racing queries.

## Contract

```text
let r = workspace.table("test2").sort([col("cat").asc()]);

table(r);
plot.bar(r, "cat", "val");
plot.scatter(r, "cat", "val")
```

1. The envelope recognizes **one contiguous trailing block of view calls**
   ending the buffer. Each statement in the block must be a recognized view
   form; the first non-view statement from the end bounds the block.
2. **Every view must reference the same relation expression** — structurally
   identical argument source (in practice, one shared `let` binding). A mixed
   set refuses with a precise Rune-source diagnostic citing both the first
   relation argument and the first differing one.
3. **Rune executes exactly once per view set.** One `ExecuteRune`, one
   bounded result, shared by every sink.
4. **View cardinality: at most one unnamed view per form** — `table`,
   `plot.bar`, `plot.scatter` — so a buffer may show up to all three at once.
   A duplicated form refuses with a diagnostic naming the deferred
   named-view decision. (Resolution of the brainstorm's "one of each kind":
   kind = form, not table/plot family — otherwise the intended
   table + bar + scatter dogfood buffer would be unreachable.) The naming
   grammar itself is deferred to its own brief discussion; scatter's optional
   category makes naïve positional naming ambiguous, so no naming spelling
   is invented here.
5. **Newest-wins scheduling treats the view set as the unit.** A buffer edit
   supersedes the entire pending set; the scheduler's census counters keep
   their existing semantics with the set as one submission.
6. **Sink failures are per-view.** A plot field that does not exist in the
   result fails that plot view, cited at its own view-call span; sibling
   views of the same result still render. Execution and relation errors map
   through the source map as today; wrapper-only failures attach to the view
   block.
7. The source map generalizes to N stripped view ranges; each view's
   original span is retained for per-view attribution.

## Rides along

`RuneViewScheduler::complete()` replaces its `debug_assert!` with a hard
invariant assertion: completing a buffer that is not in flight is a
programming error and must fail loudly in release, not silently promote
pending work.

## Tests

1. **Envelope:** the three-view block recognizes and strips correctly with
   per-view spans; a mixed-relation set refuses citing both argument spans;
   a duplicated form refuses naming the deferred naming decision; a view
   call appearing before a non-view statement is not part of the block.
2. **One execution:** the work census proves one `ExecuteRune` per set;
   every sink receives the identical stamped result (same result identity,
   not equal-by-value).
3. **Supersession:** a burst of edits against an in-flight set executes only
   the newest complete set, proven by work log; cancellation clears the
   whole set.
4. **Per-view failure:** a bad plot field fails only its plot view with the
   view-call span; the table view of the same result renders.
5. **Scheduler invariant:** a `#[should_panic]` witness proves the hardened
   assertion fires on an out-of-order completion.
6. **Live gate:** the ignored live-profile test extends to the fan-out
   buffer — one relation to table, bar, and scatter in one execution against
   the installed deployment. Human acceptance is the dogfood session that
   uses all three together (ordering item 2).

## Corpus v0.4

Add one Views row: **shared-execution view fan-out (one execution, N
sinks)** — pending, target of this pair; expansion justified by the
measurement #2 diary evidence recorded above. Existing rows unchanged.

## Non-goals

- Named views and the naming grammar (deferred to a dedicated brief
  discussion before the plotting ADR).
- Multi-relation view sets (typed refusal, revisited only on diary
  evidence).
- Any `toron`/`toron-client` change, plot-grammar extension, or streaming
  work.

## Review checklist

- [ ] One contiguous trailing view block; same-relation constraint enforced
      structurally with a two-span diagnostic.
- [ ] Exactly one execution per set, proven by census; identical stamped
      result shared by every sink by identity.
- [ ] One unnamed view per form; duplicates refuse toward the deferred
      naming decision.
- [ ] Per-view sink failures; sibling views survive.
- [ ] Whole-set newest-wins supersession and cancellation.
- [ ] `complete()` invariant hardened with a panicking witness.
- [ ] Corpus v0.4 row added; flips only with live evidence.
- [ ] Full validation green; live deployment untouched.
