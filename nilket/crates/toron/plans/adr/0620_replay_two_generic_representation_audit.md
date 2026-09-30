# ADR 620 - replay 2's generic join representation: an audit

**Status:** CLOSED (results in
`plans/impl/0620_replay_two_generic_representation_audit_results.md`).

**The representation is verified** from the current bound-plan export:
it builds `partsupp`, with the keys (`ps_suppkey`, `ps_partkey`), both
Int64 and non-nullable, not null-safe, and the key plan `Generic`.

**The SF8 envelopes** are medians of five complete runs:
- table build 789.3 ms;
- probe 1,228.7 ms;
- cleanup 556.6 ms.

**The synthetic paired differences** (observed, not amounts of removed
work):
- build, owned − inline: 198.3 ms;
- drop: 461.0 ms;
- probe, fresh − scratch: 126.9 ms.

Build plus cleanup has a per-run median of 1,345.2 ms, above the probe.

**Provisional candidate: O1, build-key storage only.** Inline
fixed-width keys; both of the successor's arms keep the per-probe
fresh-key encoding and allocation. No cut. Successor to ADR 619. **A diagnostic record: no
token, no pair, no cut, no saving projected.**

It establishes three things:
- the current representation of the scaling cell's replay-2 join (TPC-H
  query nine's composite-key join to `partsupp`);
- its cost envelopes at SF8;
- synthetic comparisons of the owned-key and per-probe-key work.

From those it may name **one provisional candidate** for a successor
record, which measures it with a controlled pair, or it reports the
choice as **unresolved**.

It keeps **build-key ownership** and **transient probe keys** separate:
- **The build encodes and allocates a key per accepted build row,**
  before insertion. One key buffer remains per distinct entry. A
  duplicate row's temporary key is freed at insertion.
- **The probe encodes and allocates a key per probe row** (the `Generic`
  branch of `probe_in_memory_join_rows_profiled` calls `join_key_bytes`).
  It wraps the key in `JoinKey::Generic`, hashes it, looks it up, and
  drops it.


## Context

**What earlier records show:**
- ADR 619's benchmark found per-row growth in a synthetic two-Int64
  `generic_hash` build of a similar order to q09's replay-2 build. It
  named replay 2 as a candidate.
- Its match to the **current** bound key is unverified. ADR 604's plan
  came from an earlier executable, and accounting parity alone does not
  establish the key.

**ADR 618's armed SF8 q09 run** (one run, identity-gated), for context:

| stage | ms |
|---|---|
| replay-2 `prep_table_build` | 835.1 |
| replay-2 `suffix_probe` (2,610,723 probe rows, all matching; its payload take 115.2 ms is recorded, lookup and expand are not) | 1,254.5 |
| replay-2 `cleanup_build` | 557.8 |

**These are envelopes, not recoverable time:**
- `cleanup_build` encloses the runtime's cleanup, not just freeing keys;
- `suffix_probe` also includes selection, match expansion, output work,
  and gaps.

This record refreshes the envelopes with five runs, and does not project
from them.


## Decision

### 1. The current representation, bound to the current plan

**The source is a same-source bound-plan export.** The harness's
`structural-census` (ADR 604) is run untimed on ADR 618's executable
(`5eadef2d…`) for q09 at SF8. It is anchored to ADR 613's sealed board,
under that board's coordinate.

It binds the query with the explain binder under the harness's planner
configuration, with no execution (`source: "explain binder
(bind_with_explain_summary), no execution"`). It writes the bound plan's
serialized `tree` and its `plan_fingerprint`. **The export's
fingerprint must equal the timed rows' q09 SF8 fingerprint** (ADR 618's
gated group), or step 1 fails and the representation stays unverified.

**What each source supplies, named precisely:**
- **The bound-plan tree** (from the census). It supplies the join node
  whose `replay_id` is 2:
  - `kind`, `strategy`, and `null_safe_keys`;
  - `left_keys` and `right_keys`, as ordinals into its children's output
    schemas;
  - each child's subtree, down to its scan's columns.

  The key columns' names, `data_type`s, and nullability come from those
  schemas. The table mapping comes from the scan's columns and table id
  within **the same tree**.

  ADR 604's census of this cell, from an earlier executable, shows such
  a node: `replay_id: 2`, `null_safe_keys: false`, `right_keys: [1, 0]`
  over an Exchange of `partsupp`'s scan `[ps_partkey, ps_suppkey,
  ps_supplycost]`, all Int64 and non-nullable. It is context until the
  current export reproduces it.
- **The join-build profile** (from step 2's attributed runs, on the same
  executable). It supplies, for runtime replay 2:
  - the key plan (`generic`);
  - build rows, distinct keys, and duplicate keys.
- **The association of plan replay 2 with runtime replay 2** requires
  four things:
  - the plan's replay ids are the runtime's;
  - the replay-2 join is the plan's only multi-column-key join, which
    forces `Generic`;
  - the profile's only `generic` record is replay 2;
  - its build rows equal the build child's table rows at SF8.

  **The build side is named by the route's own mapping,** applied to
  the exported tree (`exec/join/streaming.rs`
  `extract_reoriented_inner_chain` and `reoriented_chain_continuation`,
  at the source version of `3a782d97`, which the plan and
  implementation commits carry unchanged):
  - A stage at replay k is accepted only if its join is Inner,
    `HashPartitioned`, with no match filter and `null_safe_keys: false`.
  - The chain continues through the child that is an Exchange,
    optionally over a Filter, over the join with replay k − 1 (replay 0
    when k = 1).
  - **The opposite child is the build, with its keys:** `right` and
    `right_keys` when `left` continues; `left` and `left_keys` when
    `right` continues.

  The reader applies exactly this rule to the tree, and **fails closed**
  if replay 2's join does not fit the route. The unique `Generic` form,
  and the build rows matching the build child's 6.4 M-row table, are
  **corroboration only**. Row-count equality alone is not build-side
  identity.
- **ADR 606's cap-check receipt is not used here.** It records the
  exchange key's truncated Debug text, not the join's build keys. It can
  corroborate the wrapped stage's shape, but it does not establish the
  join key.

**Null exclusions are reported unavailable.** Ledger raw rows are counted
before filtering, and no single identified build execution's post-filter
input and null-key accounting is recorded. A separate **static**
inference, labelled as such, may state that the key columns are
non-nullable in the bound schema.

**If any association is ambiguous,** or the export's fingerprint differs,
the representation is left **unverified**, and step 4 withholds a
representation-specific choice. No production instrumentation is added
by this record.

### 2. Five armed SF8 runs: the envelopes

Five armed q09 SF8 runs on the same executable and coordinate, with node
attribution and the join-build profile, and no receipt. Each run must
show:
- status pass, and digest and fingerprint equal to ADR 618's SF8 group;
- the dataset fingerprint and settings of the declared anchor;
- the same route, and the same replay and stage topology as ADR 618;
- node-tree reconciliation, with top-level and strict completeness named
  separately;
- a build-profile record for replay 2 with its row counts;
- clean isolation (ADR 619's helpers).

Missing data is withheld, never zero. The report gives **per-run
values first,** then medians and ranges, for:
- replay 2's `prep_build` and its sub-stages;
- its `suffix_probe` and payload take;
- its `cleanup_build`;
- each run's difference from the timed median, as context.

### 3. Synthetic comparable workloads (test-only)

These are measured in ADR 619's test-only module, at SF8's replay-2 shape:
6,400,000 two-Int64 keys, validated against the SF8 receipt, and
2,610,723 probes, all present. They are **controlled synthetic
comparisons**, each with its remaining differences stated. They do not
hold "everything but one property" equal.

**C1's pair requires:**
- **identical encoded key bytes**, the production encoding;
- **identical hash input and algorithm.** The owned, inline, and
  borrowed forms all hash through **precisely the same slice `Hash`
  implementation**: each key form is hashed as the same `&[u8]` slice,
  and the table's lookups borrow that same slice form. No form feeds raw
  bytes by a different path. The derived hashes of a `[u8; N]` array and
  a `Vec<u8>` are not assumed identical.
- **the same hash seed within each paired build**, cloned from one
  `RandomState`;
- the same capacity policy and insertion order.

A fixture proves byte parity and **owned, inline, and borrowed hash
parity** under one seed. It also runs actual **successful and absent
borrowed lookups** against both tables. Checksums do not substitute for
it.

**C3** removes the per-probe buffer's allocation, reallocation (a fresh
`Vec` may grow more than once), and free work. It is not exactly one
allocation.

**Pairing:** the variants of each comparison are paired and rotated
within each independent repetition. Each pair must produce **equal
output counts and checksums**.

| comparison | A | B | the one difference | remaining differences |
|---|---|---|---|---|
| **C1: build-key ownership** | a prototype table keyed by owned `Vec<u8>` (the production encoding, 18 bytes) | the same prototype table keyed by an inline `[u8; 18]` | a heap buffer per entry against an inline key | entry size and layout, hence cache and collision placement |
| **C2: freeing** | dropping C1's A | dropping C1's B | freeing per-entry buffers | as C1 |
| **C3: probe-key allocation** | per probe: a fresh `Vec` from `join_key_bytes`, then lookup by `&[u8]` in C1's A table | per probe: encode into one reused scratch buffer, then the same lookup | the per-probe buffer's allocation, reallocation, and free work | none in the table: the lookup table and key bytes are identical |
| **Reference** | the production build and drop (`build_in_memory_join_rows_with_insert_mode`), and the production probe loop (encode, wrap in `JoinKey::Generic`, `get`) | | | |

The reference shows how close the prototypes are to production.

**The boundaries, exactly:**
- The key arrays and probe arrays are constructed outside the timing.
- The encoding is inside the timing wherever a workload performs it.
- Lookups borrow stored keys; no per-lookup cloning or wrapping, except
  in the production reference, which wraps as production does.
- Results are consumed into a checksum, and exact hit and miss counts
  are asserted.
- Representation, capacity, and accounting are validated before any
  timing.
- 5 independent builds × 5 passes, with the two variance sources
  reported separately.
- The rotated order, the fresh output file, one pinned P-core with its
  sibling idle, the executed-test gate, and the exact-output validator
  all follow ADR 619.

**These are synthetic:** prototype tables, not production code paths,
apart from the reference. **They are not query decompositions,** and
nothing here transfers a fraction onto a query span.

### 4. The choice, or unresolved

Candidates:
- **O1, build-key:** inline fixed-width keys for a `Generic` plan whose
  key columns are fixed-width and not null-safe;
- **O2, probe-key:** a reused scratch buffer and a borrowed lookup;
- **O3:** a dedicated two-Int64 key plan, which touches both.

They are ranked on four things, stated side by side:
- **the observed absolute SF8 envelopes** from step 2: the build-plus-cleanup
  envelope against the probe envelope;
- **the work each candidate removes,** shown by the synthetic comparisons
  C1, C2, and C3 in step 3;
- **feasibility:** the code touched, its generality, and its risk;
- **uncertainty.**

A **single provisional candidate** is named only if the audit supports
it:
- the current key is verified;
- the relevant envelope is clearly the larger;
- the synthetic comparison shows the candidate's removed work
  non-overlapping with the other's.

Otherwise the record reports **no selection**, and names the missing
evidence. The successor's pair, at ADR 613's coordinate with q09
deciding by ADR 588's rule, determines the real benefit. **Nothing
projects a saving.**


## Acceptance

- Step 1's census export (fingerprint-matched), and the representation or
  its named ambiguity.
- Step 2's five gated runs, with per-run values, then medians and ranges.
- Step 3's comparisons:
  - their code (test-only), with fixtures for equal outputs and
    checksums, and for exact counts;
  - ladders green on bigfoot;
  - the run on strix, under ADR 619's gates.
- Step 4's ranking, and its choice or its unresolved result.
- Plan commit, then one implementation commit.


## Guardrails

- A diagnostic: the measured path is unchanged, and the prototypes are
  test-only.
- Envelopes are never read as recoverable time.
- Synthetic results are never transferred onto query spans, and never
  projected as savings.
- No cap-check receipt in any timed or attributed run.
- The choice follows the absolute SF8 envelopes and the stated evidence,
  never scaling ratios.


## Risks

- **The prototypes differ from production's `JoinKey`-keyed `HashMap`.**
  The reference measures how far, and the successor's pair measures the
  real code.
- **The query's allocator behaviour** under concurrent work may differ
  from the benchmark's.
