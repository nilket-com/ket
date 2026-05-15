# Implementation Plan: ADR 014 — Exact Small-Set Runtime Filters

**Companion ADR:**
`plans/adr/014_exact_small_set_runtime_filters.md`

## Status

Promoted to `plans/impl/`. Implementation is pending.

This is a real cross-crate implementation: Wendelon schema
(descriptor refactor + ExactSet helpers), Toron generation
(byte-budget gate), Toron scheduler (coordinator merge),
Wendelon scan engine (kind-dispatched application), and a new
small-build proving bench row.

## Goal

Land ExactSet end-to-end as a second advisory runtime-filter
kind for hash-partitioned inner equi-joins:

- descriptor surface honest (`kind` enum on
  `RuntimeFilterDescriptor`);
- Toron generation emits ExactSet under the byte-budget gate,
  Bloom otherwise;
- coordinator merges exact fragments by sorted union, lowers to
  Bloom on budget overflow, preserves Bloom-in → Bloom-out;
- Wendelon application dispatches on `kind`, reusing the ADR 013
  hooks and helper;
- promotion evidence: small-build proving row + parity across
  all three ADR 013 application paths.

## Substrate

- `crates/wendelon/src/schema/runtime_filter.rs`
  - owns `RuntimeFilterDescriptor`, `RuntimeFilterKeyType`,
    `RuntimeFilterDropReason`, key encoding, validation, Bloom
    membership, and Arrow mask helpers.
  - Gains `RuntimeFilterKind` enum, ExactSet validation,
    sorted-union merge, exact-to-Bloom lowering, kind-dispatched
    `might_contain` / `build_runtime_filter_mask*`.
- `crates/toron/src/exec/runtime_filter.rs`
  - `MIN_FILTER_BYTES = 16 * 1024` (line 20) is the ExactSet
    budget anchor.
  - `build_runtime_filter_fragment` gains the byte-budget gate
    deciding `kind: ExactSet` vs `kind: Bloom`.
- `crates/toron/src/distributed/task.rs`
  - `RuntimeFilterFragment` carries the kind-tagged shape
    through the partition-manifest path.
- `crates/toron/src/distributed/scheduler.rs`
  - Coordinator merge gains exact-union-then-decide logic and
    exact-to-Bloom lowering on merged-overflow.
- `crates/toron/src/storage.rs`
  - `InMemoryStorageScan` keeps calling the shared kind-aware
    mask helper; no kind-specific branching at the scan boundary.
- `crates/wendelon/src/network/data_server.rs`
  - The ADR 013 application sites
    (`read_projected_query_location`,
    `read_secondary_index_query_location_group`, whole-record
    fallback) call the kind-dispatched mask helper; no new
    application sites.
- `crates/toron/tests/runtime_filter_pushdown_test.rs`
  - End-to-end runtime-filter correctness and byte-identity
    coverage extends to ExactSet.
- `crates/toron/tests/wendelon_storage_execution_test.rs`
  - In-process Wendelon harness gains the small-build ExactSet
    fixture and digest-parity coverage.
- `crates/toron/plans/impl/006_runtime_filter_pushdown_bench_results.md`
  - Receives the new small-build proving row.

## Files

Expected code changes:

- `crates/wendelon/src/schema/runtime_filter.rs`
- `crates/toron/src/exec/runtime_filter.rs`
- `crates/toron/src/distributed/task.rs`
- `crates/toron/src/distributed/scheduler.rs`
- `crates/toron/src/storage.rs` — `InMemoryStorageScan` test
  fixture / unit coverage extends to ExactSet (no kind-dispatch
  code change in `scan`; the shared mask helper handles
  dispatch).
- `crates/wendelon/src/network/data_server.rs`
- `crates/toron/tests/runtime_filter_pushdown_test.rs`
- `crates/toron/tests/wendelon_storage_execution_test.rs`
- focused unit tests under `crates/wendelon/src/schema/` and
  `crates/toron/src/`
- `crates/toron/tests/architecture_test.rs` source guards

Expected documentation:

- `crates/toron/plans/adr/014_exact_small_set_runtime_filters.md`
- `crates/toron/plans/impl/014_exact_small_set_runtime_filters.md`
- `crates/toron/plans/impl/006_runtime_filter_pushdown_bench_results.md`
- `crates/toron/plans/wip/roadmap.md`

Not expected:

- new Toron IR variants;
- new `JoinKind`, `JoinStrategy`, scheduler-barrier semantics;
- new `TaskSpec`, `TaskFailureKind`, `BudgetDimension`,
  `MessageKind`;
- changes to `TableQueryOptions`, `TableQueryRequestMeta`, or
  the ADR 013 wire frame layout;
- composite-key, broadcast, outer / semi / anti runtime filters;
- SQL `IN` lowering, PBC `InList` predicate;
- planner cost-model changes;
- row-group skip.

## Phase 0 — Audit and layout decision

- **P0.1: ExactSet payload encoding.** Choose between (a) packed
  length-prefixed canonical bytes (`u32 count` + `[len: u32,
  bytes...]*`), or (b) postcard-encoded `Vec<Vec<u8>>` (uniform
  postcard handling, slightly larger per key). Prefer (a) for
  byte efficiency unless postcard wrapping proves materially
  simpler.
- **P0.2: Budget constant.** Pin
  `EXACT_FILTER_MAX_BYTES = MIN_FILTER_BYTES` (16 KiB). The gate
  is on *payload* bytes — encoded canonical keys — not full
  serialized descriptor bytes. Add a one-line comment noting
  "ExactSet payload is payload-bounded by the smallest Bloom
  payload Toron would otherwise build; enum-tag, key-count, and
  postcard framing overhead is bounded and accepted for the
  zero-FP guarantee."
- **P0.3: Validation surface.** Enumerate ExactSet-specific
  validation paths: sorted-canonical order, dedup,
  `encoded_key_count` ↔ decoded count, payload-bytes ≤ budget,
  per-key framing well-formed. Each maps to the existing
  `RuntimeFilterDropReason::UnsupportedShape` advisory-drop
  variant. No new drop-reason variant.
- **P0.4: Lowering helper ownership.** The exact→Bloom lowering
  happens at the coordinator. Prefer a shared
  `wendelon::schema::runtime_filter::lower_exact_set_to_bloom`
  helper — keeps Bloom-construction invariants in one place.
- **P0.5: In-memory membership.** Wendelon's `kind: ExactSet`
  membership inside `build_runtime_filter_mask_for_batch`
  chooses between sorted-binary-search over the canonical bytes
  vs prepared `HashSet<Vec<u8>>` per table-query plan. Decide
  after a quick prototype measurement on the new fixture; the
  ADR contract pins behavior, not structure.
- **P0.6: Architecture guards.** Source-shape guards in
  `crates/toron/tests/architecture_test.rs`:
  - no SQL `IN` lowering in `sql_planner.rs`;
  - no `RuntimeFilterDescriptorV2` or any `V<N>` suffix type;
  - no `kind`-bypassing field access (e.g. matching on
    `RuntimeFilterDescriptor` without going through `kind`).

`// P0.x:` comments only where the audit finding is not obvious
from the code, per `plans/conventions/no-planning-phase-names.md`.

## Phase 1 — Kind-tagged descriptor and shared helpers

Refactor `crates/wendelon/src/schema/runtime_filter.rs`:

```rust
pub struct RuntimeFilterDescriptor {
    pub key_encoding_version: u32,
    pub key_types: Vec<RuntimeFilterKeyType>,
    pub probe_key_columns: Vec<u32>,
    pub probe_descriptor_fingerprint: PhysicalDescriptorFingerprint,
    pub kind: RuntimeFilterKind,
}

pub enum RuntimeFilterKind {
    Bloom(RuntimeFilterBloom),
    ExactSet(RuntimeFilterExactSet),
}
```

- Move all Bloom-specific fields into the `Bloom` variant.
- Update every construction site (Toron generator, scheduler
  merge, tests) to use the `kind` enum.
- Existing Bloom helpers operate on `&RuntimeFilterBloom`
  explicitly — no implicit kind-coercion.
- Postcard round-trip tests cover both kinds.

Add ExactSet helpers:

- validate exact payload budget, key count, sort order, dedupe,
  and key framing;
- encode and canonicalize a distinct key set;
- merge two exact sets by sorted union;
- lower an exact set into a Bloom payload;
- test encoded-key exact membership;
- build row-survival masks for Arrow batches and projected
  batch views.

Tests:

- Bloom descriptor tests pass after the shape migration.
- ExactSet round-trips through postcard.
- ExactSet rejects malformed framing, unsorted keys, duplicates,
  impossible counts, over-budget payloads.
- No false positives, no false negatives for every supported
  key type.
- Type tags remain load-bearing (`Int32(1)` and `Int64(1)`
  encode distinctly).
- NULL / unencodable probe keys are non-matches.

Lands first because it is pure refactor with the existing
ADR 013 test coverage as the Bloom regression gate.

## Phase 2 — Toron fragment generation

Update `crates/toron/src/exec/runtime_filter.rs` and
`crates/toron/src/distributed/task.rs`.

`RuntimeFilterFragment` becomes kind-tagged in parallel with
`RuntimeFilterDescriptor`.

`build_runtime_filter_fragment` algorithm:

1. Confirm ADR 006 eligibility.
2. Encode distinct non-null build keys via the shared Wendelon
   encoder.
3. Track serialized exact payload bytes.
4. If the local exact payload fits `EXACT_FILTER_MAX_BYTES`,
   emit `kind: ExactSet`.
5. If it exceeds, fall back to the existing Bloom path.

Tests:

- Small distinct build set → ExactSet.
- Duplicate-heavy build set with high row count and small
  distinct set → ExactSet (gate is bytes, not rows).
- Exact byte overflow → Bloom.
- Unsupported key types → no runtime filter (unchanged).
- Generated ExactSet has canonical sort/dedupe; no false
  negatives for inserted keys.

## Phase 3 — Coordinator merge and descriptor lowering

Update `crates/toron/src/distributed/scheduler.rs` and shared
merge helpers.

Merge rules:

- Exact + Exact → sorted union with dedupe.
- Exact union within budget → ExactSet output.
- Exact union over budget → lower union to Bloom via the P0.4
  helper.
- Bloom + Bloom → existing ADR 006 Bloom merge behavior.
- Bloom + Exact → insert exact keys into a compatible Bloom;
  output is Bloom.
- Incompatible Bloom parameters → advisory-drop / no-filter
  fallback (current behavior).

The scheduler's probe descriptor construction preserves the
selected kind and attaches the probe descriptor fingerprint
exactly as ADR 013 requires.

Tests:

- Exact fragments merge deterministically and dedupe across
  workers.
- Merged exact overflow lowers to Bloom — the resulting Bloom
  must membership-positive every key in the union (R1
  regression).
- Local-overflow Bloom + exact fragments → Bloom output, exact
  keys present.
- All-Bloom regression unchanged from ADR 006.
- Incompatible Bloom → drop, byte-identical output.
- `TaskInputBinding::Scan.runtime_filter` carries both Bloom
  and ExactSet descriptors through serde round-trip.
- Idempotency: merging X with empty = X.

## Phase 4 — In-memory scan and Wendelon application

`InMemoryStorageScan` (`crates/toron/src/storage.rs`) needs no
kind-specific branching outside the shared mask helper — it
continues to call Wendelon's validation and mask builder.

Wendelon application
(`crates/wendelon/src/network/data_server.rs`) reuses ADR 013's
order:

1. plan-time descriptor validation;
2. projected `predicate_columns` augmentation;
3. primary projected path mask before PBC
   (`read_projected_query_location`);
4. secondary-index projected path mask before PBC
   (`read_secondary_index_query_location_group`);
5. whole-record fallback mask before PBC.

The shared mask helper dispatches on `descriptor.kind`. For
`ExactSet`, kind-specific membership: each probe row's encoded
key is tested against the exact set; row survives iff present.
Any in-memory exact membership structure stays local to the
table-query plan or batch helper — not on the wire descriptor.

Tests:

- `InMemoryStorageScan` applies ExactSet after morsel slicing,
  before PBC/projection.
- Fingerprint mismatch advisory-drops ExactSet; byte-identical
  to no-filter.
- Primary projected Wendelon path fetches the exact probe key
  via `predicate_columns` augmentation while omitting it from
  PBC output.
- Secondary-index projected path applies ExactSet before PBC
  output.
- Whole-record fallback applies ExactSet before PBC output.
- Malformed ExactSet descriptors are byte-identical to absent
  filters.

## Phase 5 — Live Wendelon and bench evidence

Extend `crates/toron/tests/wendelon_storage_execution_test.rs`
with a small-build selective fixture (a few hundred distinct
keys at ~8 bytes; probe-side many rows, mixed match /
non-match). Three runs: filter-disabled, Bloom, ExactSet.
Assert digest-equal results and that ExactSet's
`false_positive_rows = 0`.

Append the proving row to
`crates/toron/plans/impl/006_runtime_filter_pushdown_bench_results.md`,
**distinctly named** from ADR 006's 100K Phase 0 row and
ADR 013's CI production row. The new row reports
filter-disabled, Bloom, and ExactSet under the same small-build
fixture; the artifact text states that ADR 006's Phase 0 row is
the *motivating* evidence (Bloom residual is material at scale)
and the new row is the *proving* evidence (ExactSet eliminates
the residual when applicable).

## Phase 6 — Documentation and roadmap

At implementation promotion (separate `feat:` commit):

- flip ADR 014 Status: "Promoted; implementation pending." →
  "Promoted and implemented.";
- flip this impl plan's Status similarly;
- update `crates/toron/plans/wip/roadmap.md` Status entry;
- the bench row landed in the previous phase;
- rotate ADR 015 candidates (lead = `GROUP BY ... ORDER BY`,
  pre-converged in the ADR 014 selection round).

## Verification

Minimum:

```text
cargo fmt --all --check
cargo check --workspace --locked
cargo test --locked -p wendelon runtime_filter --lib
cargo test --locked -p toron runtime_filter --lib
cargo test --locked -p toron --test runtime_filter_pushdown_test
cargo test --locked -p toron --test architecture_test --no-fail-fast
cargo test --locked -p toron --test wendelon_storage_execution_test --no-fail-fast
git diff --check
```

Promotion:

```text
cargo test --locked --workspace --no-fail-fast
```

The live-Wendelon ExactSet path must run in CI via the existing
in-process harness; no local-only exception.

## Risks + mitigations

- **R1: Exact→Bloom lowering loses keys.** Mitigation: a Phase 3
  merge test inserts every union-keyset member into the
  resulting Bloom and asserts `might_contain = true` for each.
- **R2: ExactSet membership false-positive via canonical-bytes
  collision.** Cannot occur if encoding is type-tagged (current
  encoder behavior). Phase 1 test pins `Int32(1)` and `Int64(1)`
  encode to distinct bytes.
- **R3: Budget set too high → table-query request size grows.**
  Mitigation: P0.2 pins `EXACT_FILTER_MAX_BYTES =
  MIN_FILTER_BYTES`; P0.6 architecture guard catches any attempt
  to raise without explicit ADR amendment.
- **R4: Old client + ADR-014 server mixed-binary.** Mitigation:
  ADR 013's existing advisory-drop on postcard-decode failure
  handles it. Mixed-binary support is not promised
  (`no-premature-backward-compat`).
- **R5: ExactSet generation hot-path regresses Bloom path.**
  The byte-budget gate requires distinct-set accounting and
  incremental serialized-payload-byte accounting up to the
  threshold, even on inputs that will fall back to Bloom. The
  Bloom *result* is unchanged; the *generation hot path* is not.
  For duplicate-heavy build inputs this is faster than today
  (smaller distinct-set work than per-row Bloom insertion); for
  unique-heavy build inputs that overflow the budget, the
  dedup-through-threshold is new work. Mitigation: Phase 2
  requires bench coverage on a unique-heavy Bloom-wins fixture
  (build cardinality well above the gate, low duplication) to
  bound the regression vs. ADR 006's existing Bloom-only
  generation. If the bench shows material regression, generation
  uses a dual accumulator that abandons exact accounting at
  budget overflow rather than continuing to dedup; the choice
  is implementation, not contract.

## Definition of done

- `RuntimeFilterDescriptor.kind` enum lands; Bloom and ExactSet
  variants present; no `V<N>` suffix anywhere.
- ExactSet and Bloom share one descriptor surface in
  `wendelon::schema::runtime_filter`.
- ExactSet validation rejects malformed, duplicate, unsorted,
  over-budget, or unsupported descriptors.
- Generated ExactSet has no false negatives and no false
  positives.
- Toron generator emits ExactSet under the byte-budget gate,
  Bloom above.
- Coordinator merge produces correct exact-union /
  exact-to-Bloom lowering / Bloom-in-Bloom-out / mixed-input
  behavior.
- Wendelon application dispatches on `kind` and reuses ADR 013's
  hooks; no new application site.
- All three ADR 013 application paths have parity coverage for
  ExactSet.
- Existing Bloom behavior remains compatible and tested.
- Live-Wendelon small-build proving row appended to the ADR 006
  bench artifact, distinctly named, showing digest-equal
  disabled/Bloom/ExactSet output and zero
  `false_positive_rows` for ExactSet.
- Architecture guards green (no SQL `IN`, no `V<N>`, no
  `kind`-bypassing field access).
- Full Toron + Wendelon workspace test suites green.
