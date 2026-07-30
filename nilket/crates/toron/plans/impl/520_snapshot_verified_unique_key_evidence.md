# impl 520 - snapshot-verified unique-key evidence plan

Infrastructure cut: the uniqueness fact ADR 519 lacked becomes available and trusted correctly
— verified at dataset sealing, bound to the bytes, carried through the manifest into a
provenance-aware `unique_key_evidence` surface on the table binding. No planner behavior
changes; the census proves it. No paired cohort; the receipt claims fact availability, not
performance.

## Immutable inputs

- tree: post-519-red main (`dfc6ea10` lineage);
- exact dataset: strict sf=8 persisted Parquet, file bytes and `full_hash` values unchanged by
  this cut; the manifest preimage version moves with the evidence extension and sf=8 reseals;
- pinned anchors: `TableBinding` + `primary_key_columns: HashSet<ColumnOrdinal>`
  (crates/toron/src/catalog.rs:854 — the conflation being left untouched, not extended);
  manifest signature/fingerprint (`manifest_signature_for_tables` /
  `manifest_fingerprint_for_tables`, crates/toron-tpch/src/lib.rs:2688 area) and
  `prepare/load_persisted_tpch_dataset`; `RowIdentityPolicy`
  (crates/wendelon/src/schema/table.rs:1211) — read-only context, no Wendelon change;
- ADR 519's engagement result: q13 fingerprint `32c7b15a1c5a244e`, digest, and 5,131,808 B
  spill identical across arms at `dfc6ea10` — the invariance baseline this cut must preserve.

## Step 1 - evidence model and consumption API (toron)

`UniqueKeyProof { columns: Vec<ColumnOrdinal>, provenance }` and
`UniqueKeyProofProvenance { EnforcedConstraint, VerifiedSnapshot }` in the catalog module;
`TableBinding.unique_key_evidence: Vec<UniqueKeyProof>` populated only from validated manifest
evidence (empty everywhere else, including every Wendelon descriptor path). Estimated NDV
remains in `TableOptimizerFacts` and cannot inhabit the proof type. The proof accessor is the
only semantic-consumer path. `primary_key_columns` and row identity remain untouched.
MockCatalog gains a fixture hook to attach proof evidence for bind-level tests.

## Step 2 - seal-time verification and manifest round-trip (toron-tpch)

At `prepare_persisted_tpch_dataset` sealing, a generic API accepts ordered
`SnapshotUniqueKeyCandidate` requests. Requests are not facts; only exact deterministic
verification emits proof. The first TPC-H request is `customer(c_custkey)`, with no
benchmark-specific branch in the verifier. Verification checks a non-null complete key under
Toron equality, uses overflow-checked counters, and fails closed on unsupported types, invalid
columns, missing files, or incomplete scans. Proof records `{table, columns (ordered),
schema_fp, file_hash, provenance: VerifiedSnapshot}` in an explicitly versioned manifest
section; the manifest fingerprint preimage extends accordingly.

`load_persisted_tpch_dataset` validates proof structure and binding against the table entry;
malformed, invalid-column, or mismatched evidence fails manifest load. It exposes
VerifiedSnapshot proofs only when the current load is `DatasetVerification::FullHash`.
`ManifestOnly` retains its measurement-only dataset load but supplies an empty proof set.
Surviving proofs flow into the gauge catalog's bindings.

## Step 3 - tests

- Round-trip: tiny sealed fixture → load → binding carries ordered key + VerifiedSnapshot;
  composite-key fixture preserves order and identity.
- Fail-closed battery: duplicates → no proof; null-bearing key → no proof; unsupported type or
  invalid candidate → seal declines; missing file / tampered proof binding / schema mismatch →
  manifest-load error; overflow and incomplete-scan guards; ManifestOnly load → no proof.
- 519 gate fixtures: bind-level probe sees VerifiedSnapshot proof for a q13-shaped
  `customer(c_custkey)` catalog; the estimated-NDV-only optimizer-fact twin finds no proof.
- Identity non-interference: row-identity suites untouched and green; no descriptor diff.
- Serialization: goldens re-pinned in-cut if the binding/IR encoding shifts; the new field
  rides all three `--all-targets` checks and the full architecture_test.

## Step 4 - reseal, invariance census, and receipt

Preserve the control sf=8 root for `dfc6ea10`; reseal a distinct candidate root for post-520.
First require every table's file hash, row count, schema fingerprint, and schema signature to
match across roots. Then run q01-q22 + m00-m10 at the respective compatible manifests — every
plan fingerprint and digest identical (q13 at `32c7b15a1c5a244e` explicitly); strict
correctness spot rails on q13 and the q19 sentinel; the gauge-side fixture confirms customer's
VerifiedSnapshot proof loads from the resealed sf=8 manifest under FullHash and is absent under
ManifestOnly. Seal
`impl: adr 520 snapshot-verified unique-key evidence` with the full gate battery and direct
exit statuses, push, and record the receipt: the fact is available, bound, provenance-correct,
and changed no plan.

## Repository gates

The standard council seal and changed-surface blocks via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.

## Implementation seal

The bounded fact path is implemented without a planner reader:

- Toron's catalog owns private, constructor-validated `UniqueKeyProof` records with ordered
  columns and a proof-only provenance union (`EnforcedConstraint` / `VerifiedSnapshot`).
  Estimated NDV remains solely in `TableOptimizerFacts`; the proof record is not deserializable
  and cannot contain an estimate.
- The TPC-H sealer supplies an explicit, untrusted `customer(c_custkey)` candidate to a generic
  ordered Int32/Int64 tuple verifier. Duplicate rows, NULL-bearing/nullable keys, unsupported
  types, invalid columns, counter overflow, and incomplete scans emit no unearned proof or fail
  the seal as appropriate.
- Manifest format 2 carries the ordered proof plus its schema/file binding in the fingerprint
  preimage. Malformed or mismatched proof metadata fails load. A FullHash load propagates the
  proof through `HarnessTableMetadata` into `TableBinding`; ManifestOnly clears all snapshot
  proofs before returning the dataset.
- The q13-shaped catalog fixture resolves `customer` with a
  `VerifiedSnapshot([ColumnOrdinal(0)])` proof. Composite order, tampered binding, duplicate,
  NULL, unsupported-type, incomplete-scan, ManifestOnly, and estimated-NDV-only cases are pinned.
  Row identity and every Wendelon source remain untouched.

The final seal battery is all green with direct statuses preserved under
`target/adr520_gates_final`: fmt checks, 1,456 Toron lib tests, 2,095 full architecture tests,
the complete `--no-fail-fast` integration suite, the full toron-tpch suite, all three
`--all-targets` checks, and `git diff --check`. The only guard reconciliation was structural:
the new source assertion lives in the carved `architecture/logical_properties.rs` module and
uses the shared binder corpus, keeping `architecture/optimizer.rs` below ADR 077's 2,500-line
ceiling.
