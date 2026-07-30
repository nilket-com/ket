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

`UniqueKeyEvidence { columns: Vec<ColumnOrdinal>, provenance }` and `UniquenessProvenance
{ EnforcedConstraint, VerifiedSnapshot, EstimatedNdv }` in the catalog module;
`TableBinding.unique_key_evidence: Vec<UniqueKeyEvidence>` populated only from validated
manifest evidence (empty everywhere else, including every Wendelon descriptor path). The proof
accessor (`semantic_proof`-style) returns evidence only for the two proof tiers; EstimatedNdv
is structurally unreachable through it. `primary_key_columns` and row identity untouched.
MockCatalog gains a fixture hook to attach evidence for bind-level tests.

## Step 2 - seal-time verification and manifest round-trip (toron-tpch)

At `prepare_persisted_tpch_dataset` sealing: exact, deterministic uniqueness verification for
the declared key columns per table — non-null complete key under Toron equality, hash-set
based, overflow-checked counters, fail-closed on unsupported types, missing files, or
incomplete scans. Evidence records `{table, columns (ordered), schema_fp, file_hash,
provenance: VerifiedSnapshot}` in a versioned manifest section; the manifest fingerprint
preimage extends accordingly. `load_persisted_tpch_dataset` re-validates each record against
the manifest's current schema_fp/file_hash and drops mismatches loudly; surviving evidence
flows into the gauge catalog's bindings.

## Step 3 - tests

- Round-trip: tiny sealed fixture → load → binding carries ordered key + VerifiedSnapshot;
  composite-key fixture preserves order and identity.
- Fail-closed battery: duplicates → no evidence; null-bearing key → no evidence; unsupported
  type → seal declines; missing file / tampered hash / schema mismatch at load → evidence
  dropped loudly; overflow and incomplete-scan guards.
- 519 gate fixtures: bind-level probe sees VerifiedSnapshot proof for a q13-shaped
  `customer(c_custkey)` catalog; the EstimatedNdv-only twin finds no proof.
- Identity non-interference: row-identity suites untouched and green; no descriptor diff.
- Serialization: goldens re-pinned in-cut if the binding/IR encoding shifts; the new field
  rides all three `--all-targets` checks and the full architecture_test.

## Step 4 - reseal, invariance census, and receipt

Reseal sf=8 (file bytes identical; manifest extended), then: baseline census from the
`dfc6ea10` release binary vs the sealed post-520 binary, q01-q22 + m00-m10 — every plan
fingerprint and digest identical (q13 at `32c7b15a1c5a244e` explicitly); strict correctness
spot rails on q13 and the q19 sentinel; the gauge-side fixture confirming customer's
VerifiedSnapshot evidence loads from the resealed sf=8 manifest. Seal
`impl: adr 520 snapshot-verified unique-key evidence` with the full gate battery and direct
exit statuses, push, and record the receipt: the fact is available, bound, provenance-correct,
and changed no plan.

## Repository gates

The standard council seal and changed-surface blocks via `scripts/cargo-cgroup`, direct exit
statuses preserved, gates re-run after any code change made in response.
