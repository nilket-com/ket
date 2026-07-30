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

1. **Evidence model, designed for the union now.** `UniqueKeyEvidence { columns:
   Vec<ColumnOrdinal>, provenance: UniquenessProvenance }` on the table binding as
   `unique_key_evidence` — column order and identity preserved, composite keys first-class
   even though q13 needs one column. `UniquenessProvenance` carries **EnforcedConstraint** and
   **VerifiedSnapshot** as semantic-proof tiers and **EstimatedNdv** as explicitly non-proof:
   the proof accessor structurally excludes it, so a semantic consumer cannot reach estimated
   evidence even by mistake. ADR 520 produces only VerifiedSnapshot; the union exists so the
   Wendelon tier lands into a stable contract instead of forcing a second migration.
2. **Exact uniqueness semantics, first cut.** A proof fact means: a unique, non-null,
   complete key under Toron's equality semantics. Nullable uniqueness, NULL-distinct
   conventions, projected/cast key expressions, and inferred functional dependencies are all
   out of scope and produce no evidence.
3. **Verification at dataset sealing, bound to the bytes.** The seal step verifies uniqueness
   exactly and deterministically over the immutable table artifact and records the evidence
   bound to that artifact's schema fingerprint and file hashes. Verification fails closed on
   unsupported types or equality semantics, missing files, counter overflow, and incomplete
   scans — no partial or probabilistic evidence exists. Loading re-validates the binding
   against the current manifest entries; evidence whose schema fingerprint or file hashes do
   not match the bytes on disk is dropped loudly — copied or stale evidence cannot attach to
   different data.
4. **Manifest round-trip.** The manifest serialization carries the key columns and
   VerifiedSnapshot provenance through load into `TableBinding.unique_key_evidence` intact.
   The manifest preimage is versioned by this extension; table file bytes and their hashes are
   untouched.
5. **Row identity is untouched.** No descriptor switches to `PrimaryKey`; identity synthesis,
   tombstone behavior, and `primary_key_columns` are byte-for-byte as today. The naming rail
   is hard: every new surface says `unique_key_evidence` (or `logical_unique_keys`), and
   nothing extends `primary_key_columns`.
6. **No consumer rewrite.** No planner decision reads the evidence in this cut; the only
   readers are the consumption API and its fixtures. The census must prove plan invariance.

## Acceptance

1. **Sealed-fixture round-trip**: a tiny dataset seals with verification, loads, and the
   binding carries the key (order preserved, composite fixture included) with
   VerifiedSnapshot provenance.
2. **Fail-closed battery**: duplicate-bearing data yields no evidence; a null-bearing key
   yields no evidence; unsupported key types decline; a missing file and a tampered
   file-hash/schema-fingerprint mismatch at load drop the evidence loudly; overflow and
   incomplete-scan guards pinned.
3. **The 519 gate fixtures, both directions**: a bind-level fixture proves a q13-shaped
   uniqueness gate can now see the verified fact for `customer(c_custkey)` through the proof
   accessor; an otherwise identical fixture carrying only EstimatedNdv evidence must still
   find no proof — the 519 lesson as a permanent structural test.
4. **Identity non-interference**: row-identity tests unaffected; the descriptor surface diff
   is empty.
5. **Census invariance**: baseline census at the pre-seal tree vs the sealed binary on the
   resealed sf=8 dataset — every plan fingerprint and digest identical across q01-q22 and
   m00-m10, strict correctness spot rails, q19 sentinel. No paired cohort: this ADR makes no
   performance claim.
6. **Serialization hygiene**: any postcard/golden vectors shifted by the binding extension
   are re-pinned in the same cut; the new field rides `--all-targets` checks.

## Guardrails

- The proof accessor is the only path to evidence, and EstimatedNdv is unreachable through it
  by construction, not by convention.
- Verification is exact or absent — no sampling, no estimates, no partial scans.
- Evidence-binding validation at load is fail-closed; absence of evidence is always safe
  (consumers decline, as 519 did).
- ADR 511-518 surfaces untouched.

## Risks

- The extended manifest preimage changes the manifest fingerprint, so sf=8 reseals; file
  hashes are unchanged and the census/correctness rails re-anchor on the resealed manifest —
  the invariance census is the proof nothing else moved.
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
