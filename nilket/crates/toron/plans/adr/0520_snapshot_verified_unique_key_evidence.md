# ADR 520 - Snapshot-verified unique-key evidence

ADR 519 closed red at its engagement gate because the fact it needed does not exist anywhere:
Wendelon's schema has no logical uniqueness surface — `RowIdentityPolicy` is an
identity-synthesis contract (the BLAKE3 preimage root), `PrimaryKeyDescriptor` exists only as
identity policy, and Toron's `TableBinding.primary_key_columns` (catalog.rs:854, a `HashSet`
that destroys column order) is fed from it — the exact identity/uniqueness conflation this cut
removes. The persisted-dataset manifest carries `rows:logical_bytes:schema_fp:file_hash` and no
uniqueness or NDV facts. The board doctrine settled after 519 makes the property layer the next
structural investment either way: it reopens 519's banked ~5.6 s, serves the existing binder,
and is a prerequisite for any future cost-based flow.

This is the bounded fork: land the **snapshot-verified tier plus Toron's provenance-aware
consumption API**, nothing else. The Wendelon declared-constraint production path is a named
cross-ring follow-on with its own justification; the q13 rewrite itself stays out — a reopened
ADR 519 cycle retains independent authority over the semantic rewrite, census, and cohort. The
receipt of this ADR establishes exactly one sentence: *the proof fact is now available and
trusted correctly.*

## Decision

1. **Proof model, designed for the proof-grade union now.** `UniqueKeyProof { columns:
   Vec<ColumnOrdinal>, provenance: UniqueKeyProofProvenance }` on the table binding as
   `unique_key_evidence` — column order and identity preserved, composite keys first-class
   even though q13 needs one column. `UniqueKeyProofProvenance` carries only the two
   semantic-proof tiers, **EnforcedConstraint** and **VerifiedSnapshot**. Estimated NDV remains
   optimizer evidence in `TableOptimizerFacts` and never inhabits the proof record at all; the
   proof accessor therefore cannot return it even by mistake. ADR 520 produces only
   VerifiedSnapshot; the proof-grade union exists so the Wendelon tier lands into a stable
   contract instead of forcing a second migration.
2. **Exact uniqueness semantics, first cut.** A proof fact means: a unique, non-null,
   complete key under Toron's equality semantics. Nullable uniqueness, NULL-distinct
   conventions, projected/cast key expressions, and inferred functional dependencies are all
   out of scope and produce no evidence.
3. **Explicit candidates; verification at dataset sealing, bound to the bytes.** Exact
   verification cannot discover arbitrary composite keys. The generic seal API therefore
   accepts ordered `SnapshotUniqueKeyCandidate` requests. A request is untrusted input, not
   evidence: only a complete exact verification emits a proof. The TPC-H harness initially
   requests `customer(c_custkey)` to reopen the banked q13 gate; the verifier contains no
   customer- or benchmark-specific branch. The seal step verifies uniqueness exactly and
   deterministically over the immutable table artifact and records the proof bound to that
   artifact's schema fingerprint and file hash. Verification fails closed on unsupported types
   or equality semantics, missing files, counter overflow, and incomplete scans — no partial or
   probabilistic proof exists.
4. **Full-hash load is part of the proof.** A loaded VerifiedSnapshot proof is available only
   when the loader actually re-hashed the current bytes and reports
   `DatasetVerification::FullHash`. `ManifestOnly` may load the dataset for its existing
   measurement-only purpose, but exposes no snapshot proofs. Present-but-malformed evidence,
   invalid columns, or evidence whose schema/file binding disagrees with its table entry is a
   deterministic manifest-load error, not a warning or a silently trusted fact.
5. **Manifest round-trip.** The manifest serialization carries the key columns and
   VerifiedSnapshot provenance through load into `TableBinding.unique_key_evidence` intact.
   An explicit manifest format version and the fingerprint preimage move with this extension;
   table file bytes and their hashes are untouched.
6. **Row identity is untouched.** No descriptor switches to `PrimaryKey`; identity synthesis,
   tombstone behavior, and `primary_key_columns` are byte-for-byte as today. The naming rail
   is hard: every new surface says `unique_key_evidence` (or `logical_unique_keys`), and
   nothing extends `primary_key_columns`.
7. **No consumer rewrite.** No planner decision reads the evidence in this cut; the only
   readers are the consumption API and its fixtures. The census must prove plan invariance.

## Acceptance

1. **Sealed-fixture round-trip**: a tiny dataset seals with verification, loads, and the
   binding carries the key (order preserved, composite fixture included) with
   VerifiedSnapshot provenance.
2. **Fail-closed battery**: duplicate-bearing data yields no proof; a null-bearing key yields
   no proof; unsupported key types decline; invalid candidate columns, a missing file, and a
   tampered proof/file-hash/schema-fingerprint binding fail manifest load; overflow and
   incomplete-scan guards pinned. A ManifestOnly load succeeds for its existing measurement
   purpose but exposes no VerifiedSnapshot proof.
3. **The 519 gate fixtures, both directions**: a bind-level fixture proves a q13-shaped
   uniqueness gate can now see the verified fact for `customer(c_custkey)` through the proof
   accessor; an otherwise identical fixture carrying only an estimated NDV optimizer fact must
   still find no proof — the 519 lesson as a permanent structural test.
4. **Identity non-interference**: row-identity tests unaffected; the descriptor surface diff
   is empty.
5. **Census invariance, two roots**: preserve the control sf=8 dataset and old manifest for the
   `dfc6ea10` binary; reseal a separate candidate root for the post-520 binary. Before comparing
   queries, require identical per-table file hashes, row counts, schema fingerprints, and schema
   signatures across roots. Then require every plan fingerprint and digest identical across
   q01-q22 and m00-m10 (q13 remains `32c7b15a1c5a244e`), plus strict q13 and q19 correctness
   rails. The old binary is never asked to validate the new manifest format. No paired cohort:
   this ADR makes no performance claim.
6. **Serialization hygiene**: any postcard/golden vectors shifted by the binding extension
   are re-pinned in the same cut; the new field rides `--all-targets` checks.

## Guardrails

- The proof accessor is the only semantic-consumer path to unique-key proofs. Estimated NDV
  remains a separate optimizer fact and cannot inhabit the proof type.
- Verification is exact or absent — no sampling, no estimates, no partial scans.
- Evidence-binding validation at load is fail-closed; ManifestOnly loads expose no snapshot
  proofs; absence of proof is always safe (consumers decline, as 519 did).
- ADR 511-518 surfaces untouched.

## Risks

- The extended manifest format and preimage change the manifest fingerprint, so sf=8 reseals.
  The pre-520 binary cannot validate the new manifest; the two-root census first proves the
  Parquet artifacts identical, then compares plans/results at their respective compatible
  manifests.
- The evidence surface touches `TableBinding` (widely constructed); the golden/`--all-targets`
  discipline from prior IR-field lessons applies.
- Scope creep toward the q13 rewrite is explicitly barred: any planner reader beyond the
  fixtures fails this ADR's own invariance census.

## Forward

- A reopened ADR 519 cycle consumes the fact with independent census and cohort authority.
- The Wendelon declared-constraint production path (EnforcedConstraint tier) is its own
  cross-ring ADR on the storage ring's justification.
- EstimatedNdv population, if ever, belongs to optimizer-facts territory and its own rules
  (the adr136 cross-ring marker tripwire is noted for that successor).
