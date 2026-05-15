# ADR 014: Exact Small-Set Runtime Filters

**Author:** AI Swarm Council (Claude / Codex / Gemini)
**Primary draft:** Codex iter-4
**Companion implementation plan:**
`plans/impl/014_exact_small_set_runtime_filters.md`

## Status

Promoted to `plans/adr/`. Implementation is pending.

Three-way AI Swarm Council convergence reached across five
iteration rounds. Iteration artifacts retire when this plan is
promoted — git holds the narrative.

ADR 014 owns no independent bench artifact. Its evidence lands in
`plans/impl/006_runtime_filter_pushdown_bench_results.md`
alongside ADR 013's production row — ADR 014 completes the
post-ADR-013 wire-byte-reduction story for small build sets.

Load-bearing antecedents: Codex iter-1/2/3's narrow framing
(runtime-filter `kind` enum, not SQL `IN`, not PBC `InList`, not
a new join surface) and iter-2/3 resolution of the merge rule
(all-exact workers unless local overflow; coordinator decides
exact-vs-Bloom on the merged union); Claude iter-2's surfacing
of the load-bearing FP-residual evidence in the ADR 006 Phase 0
row (1.0M FP among 1.997M survivors at 100K×100M scale — both
of the roadmap's promotion gates for exact small-set runtime
filters clear); Claude
iter-3's three precision points (advisory-drop inheritance from
ADR 013 stated explicitly, the distinct small-build promotion
fixture, the full house-style Status); Gemini iter-1's
Candidate-C lead and iter-2 stale-information concession that
surfaced the V1-vs-V1.x framing question the FP evidence then
resolved.

## Goal

Add an exact small-set runtime-filter tier for hash-partitioned
inner equi-joins.

ADR 006 introduced advisory Bloom runtime filters. ADR 013 made
those filters live at the storage-scan boundary. ADR 014 adds an
`ExactSet` descriptor kind for build-key sets small enough to
ship exactly. When applied, ExactSet filters have zero false
positives; when an exact set is too large, Toron falls back to
the existing Bloom behavior.

## Evidence

ADR 006's full Phase 0 selective row is the deciding evidence:

- build rows: `100,000`;
- probe rows: `100,000,000`;
- true match rows: `1,000,000`;
- Bloom-filtered probe rows: `1,997,130`;
- implied false positives: `997,130`;
- implied false-positive rate: ≈ `1.01%`.

The Bloom filter is correct and matches its target. The residual
is still material: about half of the rows surviving the Bloom
filter are false positives. ADR 013 proves the production
Wendelon application path exists; ADR 014 attacks the measured
residual on small exact-fit build sets.

## Scale boundary

**Primary:** cross-ring runtime-filter descriptor semantics.

ADR 014 touches:

- Wendelon's shared runtime-filter descriptor and membership
  helpers (`wendelon::schema::runtime_filter`);
- Toron's runtime-filter generation and build-fragment merge
  path (`toron::exec::runtime_filter`,
  `toron::distributed::scheduler`);
- Wendelon's row-time runtime-filter application helper inside
  the three ADR 013 hooks (`read_projected_query_location`,
  `read_secondary_index_query_location_group`, whole-record
  fallback);
- live Toron-to-Wendelon evidence and the ADR 006 bench
  artifact.

Explicitly unchanged:

- ADR 006 generation / merge / carrier / two-wave barrier
  *semantics* (only the descriptor's internal shape evolves);
- ADR 013 wire frame (`TableQueryRequestMeta.runtime_filter_len`,
  trailing `[PBC bytecode][postcard descriptor]` split);
- ADR 013 application hooks and order (after morsel/range
  narrowing and descriptor verification, before PBC projection);
- hash-partitioned inner equi-join eligibility — no broadcast,
  outer, semi, anti, non-equi, or sort-merge;
- single-column key restriction; the current
  `RuntimeFilterKeyType` set (`Int8/16/32/64`,
  `UInt8/16/32/64`, `Utf8`); no composite keys;
- `TableQueryOptions` remains `Copy`; descriptor stays on
  `TableQueryRequest`;
- Toron IR / `JoinKind` / `JoinStrategy` / scheduler barrier /
  `TaskSpec` / `TaskFailureKind` / `BudgetDimension` /
  `MessageKind`;
- SQL `IN` lowering, PBC `InList`, row-group skip, planner
  cost-model opt-in;
- aggregate table-query runtime-filter semantics.

No slot, field, message, PBC op, or descriptor surface is
reserved for future runtime-filter extensions (broadcast,
outer/anti, composite-key, encoded-type widening). Those ADRs
add their own surface when they pin their own contract.

## Decision

### 1. `RuntimeFilterDescriptor.kind` — tagged enum

The descriptor splits its Bloom-specific fields into a
`RuntimeFilterKind` variant:

```rust
pub struct RuntimeFilterDescriptor {
    pub key_encoding_version: u32,
    pub key_types: Vec<RuntimeFilterKeyType>,
    pub probe_key_columns: Vec<u32>,
    pub probe_descriptor_fingerprint: PhysicalDescriptorFingerprint,
    pub kind: RuntimeFilterKind,
}

pub enum RuntimeFilterKind {
    Bloom {
        hash_seed: u64,
        bit_count: u32,
        hash_count: u8,
        byte_budget: u32,
        target_fp_rate_bp: u16,
        payload: Vec<u8>,
    },
    ExactSet {
        byte_budget: u32,
        encoded_key_count: u32,
        encoded_keys: Vec<u8>,
    },
}
```

No `V2` wrapper (`plans/conventions/no-version-suffixes.md`).
Common validation (`key_encoding_version`, `key_types` ↔
`probe_key_columns` arity, fingerprint) stays on the outer
struct; kind-specific validation lives in the kind's helper.
Concrete `encoded_keys` layout (packed length-prefixed vs nested
`Vec<Vec<u8>>`) is a Phase 0 audit item; the contract requires
only that the bytes are canonical: sorted by encoded-byte order,
duplicate-free.

The table-query wire is unchanged from ADR 013: trailing payload
remains `[PBC bytecode][postcard RuntimeFilterDescriptor]`,
split by `bytecode_len` + `runtime_filter_len`. No-filter
clients with `runtime_filter_len = 0` stay naturally compatible.

### 2. ExactSet is advisory — inherits ADR 013's principle

ExactSet is an optimization tier, not a semantic dependency.
Dropping an ExactSet descriptor returns a superset and must be
byte-identical to filter-disabled execution after the downstream
join. This inherits **ADR 013's advisory-drop principle
verbatim** — categorically unlike the ADR 010/011/012 *semantic*
drops, which silently changed observable query results. An
advisory mask drop returns a superset the join filters
identically; ADR 014 is not a fourth member of the silent-drop
integrity sequence.

Malformed, unsupported, oversized, stale, or partially
understood ExactSet descriptors advisory-drop. They must not be
partially applied. Fingerprint validation runs before any
membership evaluation.

### 3. Eligibility — identical to ADR 006/013

ExactSet generation is eligible only when Bloom generation is
already eligible:

- hash-partitioned inner equi-join;
- single-column key;
- current supported `RuntimeFilterKeyType` set;
- no composite, float, date, timestamp, decimal, or unsupported
  key type.

Broadcast joins, outer joins, semi joins, anti joins, sort-merge
joins, and non-equi joins remain separate future ADRs.

### 4. Generation — byte-budget gate

Toron's `build_runtime_filter_fragment` collects distinct
canonical encoded build keys while the exact payload remains
within budget. The budget rule:

> **ExactSet fires only when the serialized exact payload bytes
> ≤ Bloom's `MIN_FILTER_BYTES`** (currently 16 KiB in
> `toron::exec::runtime_filter`).

The gate is on *payload* bytes — encoded canonical keys — not on
full descriptor bytes. The ExactSet payload is therefore never
larger than the smallest Bloom *payload* Toron would otherwise
build; the descriptor's enum-tag, `encoded_key_count`, and
postcard framing overhead are bounded and accepted in exchange
for zero false positives. Above the gate, Bloom's
space-amortization wins. Phase 0 may tune the constant once we
have ExactSet-fixture evidence, but the *principle* —
"ExactSet's payload is payload-bounded by the smallest Bloom
Toron would otherwise build, so it cannot grow unboundedly
relative to Bloom" — is pinned. The promoted contract must not
allow ExactSet payloads near Bloom's 1 MiB cap without new
evidence.

The gate is *serialized encoded-key bytes*, not raw build row
count. Duplicate build keys do not consume repeated exact
payload bytes after dedupe.

### 5. Coordinator merge — all-exact workers, coordinator decides

- Workers emit `kind: ExactSet` whenever the local distinct
  encoded set fits the budget. A worker whose *local* set
  exceeds the budget falls back to Bloom locally — since the
  global merged set is at least as large as any local distinct
  set, the merged ExactSet could not fit either.
- Coordinator merges all-exact input by sorted union and dedupe.
  If the merged union fits the budget, the probe wave receives
  `kind: ExactSet`. If the merged union exceeds budget, the
  coordinator lowers the union to Bloom by feeding the union
  through Toron's existing Bloom generator.
- **Bloom in → Bloom out.** Any worker emitting Bloom forces
  Bloom output. Exact fragments may be inserted into a
  compatible Bloom merge. Incompatible Bloom parameters use the
  existing advisory-drop / no-filter fallback.

This bounds worker-to-coordinator manifest size and preserves
the *optimization-tier* property: ExactSet never makes
high-cardinality joins worse than ADR 006/013 Bloom.

### 6. Canonical wire payload

ExactSet payloads are canonical encoded keys:

- sorted lexicographically by canonical encoded bytes;
- duplicate-free;
- length-framed or otherwise unambiguously decodable;
- bounded by `byte_budget`;
- carrying an `encoded_key_count` or equivalent consistency
  check.

Validation rejects malformed key framing, unsorted payloads,
duplicates, impossible counts, and payload lengths that exceed
budget. Validation/drop reasons reuse the existing closed
`RuntimeFilterDropReason` set from ADR 013 — chiefly
`unsupported_shape` for ExactSet-specific malformation. No new
drop-reason variant.

### 7. Membership semantics

For a valid ExactSet descriptor:

- generated encodable build keys must never false-negative;
- probe keys not present in the encoded set must not
  false-positive;
- NULL or unencodable probe keys are row-level non-matches;
- key type tags remain load-bearing (`Int32(1)` and `Int64(1)`
  do not collide as logical keys — they encode to distinct
  bytes).

Wendelon may prepare any internal membership structure per
table-query plan or batch (sorted binary search, prepared hash
set, or another verified structure). The wire contract is
canonical sorted bytes; the runtime implementation choice is
not part of the contract.

### 8. Application — same hooks, kind-dispatched membership

ExactSet reuses ADR 013's application order verbatim:

1. after morsel/range narrowing;
2. after table descriptor verification;
3. after projected descriptor matching;
4. before PBC projection;
5. before downstream row/byte accounting that observes scan
   output.

Hooks: `read_projected_query_location`,
`read_secondary_index_query_location_group`, whole-record
fallback — the ADR 013 shared mask helper extends to dispatch
on `descriptor.kind`. Projected reads still augment
`ColumnFetchRequirement::predicate_columns` with runtime-filter
probe columns. Probe key columns fetched for exact membership
must not appear in output unless the query requested them.

### 9. Metrics — inherit ADR 013's set

The descriptor-absent path emits no runtime-filter metrics.
Descriptor-present scans reuse ADR 013's bounded set (`present`,
`applied`, `advisory_dropped`, `drop_reason`, `input_rows`,
`surviving_rows`). A bounded `kind` label with the closed set
`{bloom, exact_set}` may be added if it matches the existing
metrics-registry style. Descriptor fingerprints, table IDs, key
ordinals, payload bytes, and exact key values are not metric
labels.

## Consequences

ExactSet can reduce probe rows crossing the
Wendelon-to-Toron boundary below Bloom when build cardinality is
small enough to ship exactly. It does not replace Bloom — Bloom
remains the scalable fallback for larger build sets and
mixed-fragment cases.

The descriptor shape change is cross-crate but contained: Toron
and Wendelon already share `RuntimeFilterDescriptor` via
`wendelon::schema::runtime_filter`, and ADR 013 already routes
that descriptor through table query. The implementation updates
every construction, validation, and membership site together.

## Promotion evidence

Implementation promotion requires:

1. Shared Wendelon schema tests proving ExactSet descriptor
   roundtrip, canonical sort/dedupe validation, malformed
   payload rejection, no false positives, and no false negatives
   for every supported key type.
2. Toron generation tests proving ExactSet is selected when
   serialized encoded-key bytes fit budget and Bloom is selected
   when they do not. Gate is on serialized bytes, not row count
   — a duplicate-heavy build with high row count and small
   distinct set must still emit ExactSet.
3. Coordinator merge tests proving exact union,
   merged-overflow exact-to-Bloom lowering, local-overflow Bloom
   fallback, and mixed exact/Bloom input behavior.
4. Wendelon primary projected, secondary-index projected, and
   whole-record application tests matching ADR 013's coverage
   for ExactSet.
5. Advisory-drop parity tests proving malformed ExactSet
   descriptors produce byte-identical output to filter-disabled
   execution.
6. Live Toron-to-Wendelon coverage proving filter-disabled,
   Bloom, and ExactSet outputs are digest-equal via the existing
   `wendelon_storage_execution_test.rs` in-process harness.
7. **Two bench rows in
   `plans/impl/006_runtime_filter_pushdown_bench_results.md`,
   named distinctly:**
   - The existing 100K-build Phase 0 row stays as *motivating*
     evidence (Bloom residual is material at scale; ExactSet
     does not fit this fixture's budget).
   - A new small-build *proving* row — build small enough to fit
     the ExactSet budget (a few hundred to ~1K distinct keys at
     8–16 bytes each) — reporting filter-disabled, Bloom, and
     ExactSet results. ExactSet must show zero
     `false_positive_rows` and at least as few storage-boundary
     rows/bytes as Bloom on the same fixture.

## What not to do

- Do not add SQL `IN` lowering.
- Do not add a PBC `InList` predicate.
- Do not add row-group skip.
- Do not extend runtime filters to broadcast, outer, semi, or
  anti joins.
- Do not add composite-key ExactSet support.
- Do not add planner cost-model opt-in.
- Do not put runtime-filter descriptors into `TableQueryOptions`.
- Do not add version-suffixed descriptor types
  (`no-version-suffixes`).
- Do not silently widen runtime-filter eligibility beyond
  ADR 006.
- Do not smuggle ExactSet payload bytes through Bloom-shaped
  descriptor fields by sentinel values — the `kind` enum exists
  so that does not happen.
- Do not raise the ExactSet budget toward Bloom's 1 MiB cap —
  the gate is the contract.

## ADR 015 handoff

`GROUP BY ... ORDER BY` is the pre-converged ADR 015 lead
candidate. All three council members named it with the same
three-stage fragmenter shape in the ADR 014 selection round:
scan/projection producers → group-keyed exchange into grouped
aggregate consumers → terminal gather exchange into one final
`Sort` task. Composes ADR 010 (`Sort`) and ADR 011 (`Aggregate`)
with no new IR variant; planner reject is at
`sql_planner.rs:98`. Gemini iter-2 opened two real iter-1
questions for that cycle: terminal-gather representation in
scheduler / fragmenter, and the `lower_query` seam at the
reject site.
