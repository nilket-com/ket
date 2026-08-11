# ADR 83 — range-distribution exposure measurement, 2026-08-11

**Result: there is no no-facts window. The measurement was not run, because
instrument validation showed neither the proposed instrument nor any real
deployment could express the change.**

The planned comparison was `a542ced9` (parent) against `c4177470` (the cut that
suppressed v1 range-distribution facts), same dataset, host, configuration,
query corpus, and perf-rail procedure. Validation ran first. It falsified the
premise, so no timing table exists and none should be quoted.

## What was checked, in order

### 1. The proposed instrument cannot see the change

`toron-tpch` builds its own optimizer facts and never reaches Wendelon's
statistics pipeline:

- `facts_for_batch` (`toron-tpch/src/lib.rs:1535`) sets `row_upper_bound` and
  `byte_upper_bound`, then `..Default::default()`.
- `table_facts_from_manifest` (`:2446`) sets those two plus `column_range`, then
  `..Default::default()`.

`column_range_distributions` is therefore empty on both sides of the boundary.
The harness uses `MockCatalog`, so `range_distribution_fact_for_column` — the
function `c4177470` suppressed — is never called.

Note the near-miss: `column_range` **is** populated, from parquet footer
statistics under ADR 471. That is min/max `RangeFact`, a different fact class,
untouched by this cut. A comparison quoting tpch plan stability would have been
reporting on the fact class that did not change.

Running it would have produced identical plans by construction and licensed a
false "no material change" — a signal that cannot express the failure it is
cited against.

### 2. No production table can produce the fact at all

Range-distribution facts are built only for columns named in
`descriptor.stats_policy.range_distributions`
(`stats_facts.rs::range_distribution_column_candidates`). That list is empty
everywhere outside tests. Three independent lines agree:

- **No spec is ever constructed outside tests.** The only
  `ColumnRangeDistributionSpec::with_k` calls are `storage/stats.rs:4359` and
  `:4997`; that file's first `#[cfg(test)]` is line 3021, so both are
  test-scope.
- **No production code initializes the policy.** Every non-test
  `range_distributions:` initializer is `Vec::new()`. The single production
  `stats_policy:` occurrence (`hash/descriptor_fingerprint.rs:86`) is a function
  parameter, not an initializer.
- **No authoring surface exists.** Neither `sql_planner.rs`, `rune_engine.rs`,
  nor the Wendelon client mentions `stats_policy`; `range_distribution` appears
  in none of them. There is no DDL, client API, or config path by which a table
  could declare one.

### 3. The pre-cut writer was already looking up an empty list

Before `c4177470`, `storage/stats.rs` selected the spec by name from
`descriptor.stats_policy.range_distributions`. After, it is a hardcoded `None`
with an ADR 0083 comment. Because the policy is empty for every production
table, both revisions produce the same thing: no range-distribution payload in
any CSB block.

**The suppression is behaviourally a no-op outside tests.**

## Consequences

- **CSB v2 does not jump the queue.** Nothing went dark, so there is no live
  degradation to bound, and no urgency argument from restoring service.
- **CSB v2's value changes shape.** It is not "restore a fact class that was
  lost"; it is "make a declared-but-unreachable fact class reachable for the
  first time." That is a feature with a fleet-fence cost, and it should be
  scheduled and justified as one — including whether an authoring surface is
  wanted at all, since without one the fact class stays empty after v2 too.
- **Two claims are corrected.** The review note at `c4177470` that "the no-facts
  window is now live, so plan shapes and timing baselines shift from this commit
  forward" was wrong; no plan shape or baseline moved. ADR 0083's evidence
  section correctly describes the CSB path *as a code path* reaching binder
  selectivity, but that path was never fed in production, so the live
  selectivity exposure was nil. The ordered-arrival defect was real and remains
  real for the **aggregate** path that ADR 546 now uses; only the optimizer-fact
  exposure was theoretical.
- **The two queued repairs keep their own justification.** The
  `FragmentPlanConfig::new` placeholder seed and `from_legacy_levels`' constant
  derivation are unaffected by this result and stand on the aggregate path.

## 4. The preserved-profile question, and how it was actually closed

The open risk was a descriptor persisted by an *earlier* revision carrying a
non-empty policy, since stored descriptors are deserialized rather than
reconstructed. Two routes were considered.

### The byte scan is a null instrument — its zero is not evidence

A read-only scanner walked all six preserved profiles under
`~/.local/share/nilket/` (`confirm0081`, `confirm0082`, `confirm0539`,
`confirm0541`, `local`, `nilket` — 38,957 `.wnd` files, ~1.4 GB), opening every
file `rb` only, looking for the CSB magic `WNDSTAT1` and testing each column
entry's `stat_flags` for `COLUMN_STAT_HAS_RANGE_DISTRIBUTION` (`1 << 10`).

It found **zero CSB blocks** — and therefore zero range-distribution entries.
That zero is **not** a negative result. Slab payloads are encrypted at rest:
files carry a `BALS` header followed by high-entropy content, so the CSB magic
is not visible to a raw read at all. The scan cannot distinguish "no range
distributions" from "cannot see anything". Reading the blocks would require
going through the storage reader, which is exactly the writer-adjacent path this
check was required to avoid.

**No one should cite this scan as confirmation.** It is recorded only so the
attempt is not repeated in the belief it succeeded.

### The history search is what closes it

A descriptor can only carry a non-empty policy if some revision was able to
write one. Searching the full history rather than the profiles:

- `git log -S "range_distributions: vec!"` returns exactly **four** commits ever:
  `4d9ce0b1` (ADR 159 producer), `83208023` (ADR 158 consumer), `825b5385`
  (ADR 078 slab-manager decomposition), `55b990d1` (ADR 176 binder
  decomposition). The latter two are decompositions that relocated the same
  code.
- In every one, the non-empty initializer is the test fixture
  `ColumnRangeDistributionSpec::with_k("amount", k)`. `amount` is a fixture
  column, and the sites are test-scope.
- `git log -S "ColumnRangeDistributionSpec::"` returns only the producer commit
  and the decomposition that moved it.
- No revision introduced a DDL, client, or config authoring surface.

No revision has ever been able to persist a non-empty
`stats_policy.range_distributions` from production code. The preserved profiles
therefore cannot contain one, and this holds for every profile written by any
build of this repository — a stronger statement than inspecting six directories
would have supported.

### What would still falsify it

A descriptor written by a build outside this repository's history, or one
hand-edited after the fact. Neither is claimed to be ruled out.

## Provenance

| Property | Value |
| --- | --- |
| Boundary | `a542ced9` → `c4177470` (direct parent) |
| Method | source determination plus full-history search; no timing run performed |
| Timing runs | none — deliberately, see §1 |
| Profile scan | attempted read-only over 6 profiles / 38,957 `.wnd` files; **null instrument**, see §4 |
| Profiles modified | none; every file opened `rb`, no lock taken, no writer invoked |
| Materiality rule | not applied; no measurement to judge |
| Working tree | clean at `24cde28c` |
