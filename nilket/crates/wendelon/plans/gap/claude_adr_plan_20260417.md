# ADR Production Plan
**Date:** 2026-04-17
**Lead:** Claude
**Reviewers:** Codex, Gemini
**Source of decisions:** [claude_answers_20260417.md](/home/me/work/wendelon/plans/gap/claude_answers_20260417.md), [codex_answers_20260417.md](/home/me/work/wendelon/plans/gap/codex_answers_20260417.md), [gemini_answers_20260417.md](/home/me/work/wendelon/plans/gap/gemini_answers_20260417.md), [open_questions_20260417.md](/home/me/work/wendelon/plans/gap/open_questions_20260417.md)

After three rounds of answer revisions the three answer sets have converged. The next step is to freeze the convergences into the repo's ADR substrate so implementation workstreams have a durable reference. This document specifies **which ADRs to write, in what order, who reviews each, and how they interact with the existing ADR set.**

The ADR **artifacts** are written under `plans/adr/` using the existing `plans/adr/TEMPLATE.md`. This document is the meta-plan — it does not itself contain ADR content.

---

## Principles

1. **One decision per ADR.** If a question can be answered independently, it gets its own ADR. Composite ADRs become unreviewable and un-supersedable.
2. **ADRs capture the decision and the rejected alternatives.** Implementation detail lives in `plans/<subsystem>/`. Cross-link both ways.
3. **Supersede, never silently edit.** When a new ADR changes a prior decision, set the prior ADR to `Superseded by NNN` and link. Never rewrite history.
4. **Every ADR cites its answer-file source.** The per-section convergence is the input; the ADR is the canonical output. This gives later readers a paper trail.
5. **No ADR ships without a reviewer from each of the other two perspectives.** Codex and Gemini review every ADR I author. One approving review per reviewer is the exit bar.

---

## ADR inventory

Next available number is **015** (existing ADRs run 001–014). Fourteen new ADRs proposed, grouped into five waves.

| # | Title | Answer-file source | Supersedes / amends | Wave | Reviewers |
|---|-------|---------------------|---------------------|------|-----------|
| 015 | Segmented WAL and snapshot lifecycle | §3 | Amends 007 (no supersession) | 1 | Codex, Gemini |
| 016 | Durability model: explicit fsync group-commit | §9 durability | — | 1 | Codex, Gemini |
| 017 | Persistent local index on redb | §9 persistent index | — | 1 | Codex, Gemini |
| 018 | Dual-CA mTLS identity and OID-bound authorization | §6 authz | — | 2 | Codex, Gemini |
| 019 | Cipher selection and FIPS posture | §5 cipher, §5 FIPS | — | 2 | Codex, Gemini |
| 020 | L2 envelope encryption and DEK lifecycle | §5 granularity, L2 mode, caching, migration | — | 2 | Codex, Gemini |
| 021 | Tenant-aware architecture from day one | §10 all | Supersedes §2 (JWT/RBAC) of `plans/ops/multi_tenancy.md`; replaces §1's "unified ring + `Hash(NamespaceID \|\| RawKey)`" with per-tenant first-class keyspaces | 3 | Codex, Gemini |
| 022 | L2 slab format and footer-indexed range reads | §4 range reads, index, ObjectStore break | Amends 006 | 4 | Codex, Gemini |
| 023 | L2 upload lifecycle (Raft-authoritative + local ledger) | §4 pending-upload, multipart | Amends 006 | 4 | Codex, Gemini |
| 024 | L2 orphan cleanup and remote reclaim | §4 orphan authority, remote reclaim | Amends 006 | 4 | Codex, Gemini |
| 025 | Audit event model and tamper-evident hash chain | §6 audit sink, audit tamper | — | 5 | Codex, Gemini |
| 026 | FIPS 140-3 certification target and module boundary | §6 FIPS | — | 5 | Codex, Gemini |
| 027 | Observability posture: OTLP, tracing stack, health/metrics split | §7 tracing, health, metrics, depth | — | 5 | Codex, Gemini |
| 028 | Deterministic simulator and `AsyncFileOps` abstraction | §8 all | — | 5 | Codex, Gemini |

### Questions that do **not** become ADRs

- **§1 Integration gate & bring-up** — test infrastructure, not architecture. Lands in `plans/ops/testing.md` as an update, not an ADR.
- **§2 Immediate correctness fixes** — implementation tranche, tracked in workstream documents, not architecture.
- **§7 CLI scope (`wendelon-ctl` command surface)** — product-surface inventory; belongs alongside `plans/ops/lifecycle.md`, not in an ADR. The CLI *packaging* (separate binary) is already implied by ADR 007-era decisions and doesn't need a new ADR.
- **§9 Memory admission control** — design shape is clear (semaphore byte-permits, RAII); lands as an implementation note in the resource-control workstream. Promote to an ADR only if the design changes materially during prototyping.

These exclusions are deliberate — not every converged answer needs an ADR. The ones listed are operational docs or implementation tranches, not architectural decisions.

---

## Waves and dependencies

Waves are ordered by dependency, not effort. Each wave must complete review before the next wave starts drafting, because later waves cite earlier ADRs as prerequisites.

### Wave 1 — Durability and consensus storage foundations
**ADRs 015, 016, 017.** These underpin every later ADR that discusses on-disk state. No upstream dependencies; can be drafted in parallel.

Exit criteria: all three accepted; `plans/consensus/` cross-references updated.

### Wave 2 — Identity and cryptography
**ADRs 018, 019, 020.** ADR 018 (dual-CA mTLS) is the identity root that Wave 3 (tenancy) and Wave 4 (L2 encryption-in-depth) depend on. ADR 019 (cipher/FIPS) is the crypto-primitive root that ADR 020 depends on. Draft 018 and 019 in parallel; draft 020 after 019 is accepted.

Exit criteria: 018, 019, 020 accepted; `plans/network/security.md` updated; `plans/schema/` reflects the cert OID layout.

### Wave 3 — Tenancy
**ADR 021.** Depends on 018 (cert-bound identity is the tenancy root). ADR 021 explicitly supersedes the JWT-based RBAC model in `plans/ops/multi_tenancy.md` §2, and replaces §1's shared-ring tenant-salted hashing (`Hash(NamespaceID || RawKey)`) with per-tenant first-class keyspaces. §3 (resource quotas) stands with minor alignment to the Raft-state-machine framing in ADR 021.

Exit criteria: 021 accepted; `plans/ops/multi_tenancy.md` updated with supersession notice and rewrite.

### Wave 4 — Tiered object storage
**ADRs 022, 023, 024.** All three amend ADR 006 rather than supersede it — 006's core framing (L2 exists, migration is lazy) stands; the new ADRs add the concrete layout, upload lifecycle, and reclaim protocols that 006 left open. Depends on 020 (L2 envelope encryption) because slab layout, upload, and reclaim all interact with DEK placement.

Exit criteria: 022, 023, 024 accepted; ADR 006 updated with "Extended by 022-024" cross-links; `plans/ops/storage_tiering.md` rewritten.

### Wave 5 — Operational surface
**ADRs 025, 026, 027, 028.** Independent of each other; can draft in parallel. Depends on Wave 2 (audit-chain signatures use §5/§6 crypto; observability depends on identity for authenticated metrics endpoints).

Exit criteria: all four accepted; `plans/ops/observability.md` updated to align with 027; `plans/ops/testing.md` updated to align with 028.

---

## Per-ADR scope notes

Enough detail per ADR for a drafting author to start without re-deriving scope. Not enough to preempt the ADR content itself.

### ADR 015 — Segmented WAL and snapshot lifecycle
- **In:** fixed-size segment files, drop-below-snapshot compaction, both log-count and wall-clock snapshot triggers, 30-second replay target, fixed-size 4 MiB snapshot chunks with manifest-at-end, zstd with dictionary training, checkpoints at snapshot boundary + periodic.
- **Out:** redb index (→ 017), encryption at rest (→ 019, 020), storage device abstraction (→ 028).
- **Unresolved → record as open:** exact segment size (128 MiB default proposed, needs benchmark), snapshot threshold defaults on reference hardware, concrete compression ratio measurement.
- **Rejected alternatives:** hole-punch compaction, rewrite compaction, semantic snapshot boundaries, lz4 compression.

### ADR 016 — Durability model
- **In:** explicit `fsync`/`fdatasync` at consensus-ack boundaries, adaptive group-commit window (200µs floor / 2ms ceiling), immediate flush on administrative barriers.
- **Unresolved → record as open:** exact scaling-factor constants inside the clamp (measured, not guessed).
- **Rejected alternatives:** PLP-assumption, fixed-window group commit, no group commit.

### ADR 017 — Persistent local index on redb
- **In:** redb as embedded ACID index, write batching aligned to WAL group-commit boundaries, encryption layer above redb, full-scan fallback as corruption-recovery path.
- **Rejected alternatives:** sled (maintenance profile), custom on-disk format (owning a decade of tail bugs), periodic checkpoint-file index (no transactional guarantees).

### ADR 018 — Dual-CA mTLS identity and OID-bound authorization
- **In:** Node CA and Operator CA as two trust roots; CBOR-encoded OID extension carrying `{role, scopes, tenant_id, key_id}`; four pilot roles (`data_node`, `operator`, `read_only_observer`, `break_glass_admin`); deny-by-default on mutating RPCs; rate-limiting composition (per-IP pre-auth → per-identity post-auth).
- **Out:** audit record format (→ 025), FIPS posture (→ 019, 026), tenancy schema (→ 021).
- **Rejected alternatives:** single CA with OID-only role separation, bearer tokens as primary primitive, per-role-per-CA factoring.

### ADR 019 — Cipher selection and FIPS posture
- **In:** AES-256-GCM-SIV preferred; decision tree for non-SIV backends — XChaCha20-Poly1305 for non-FIPS builds, plain AES-256-GCM with structural nonces (`slab_id || chunk_index`) only when FIPS required *and* SIV unavailable; aws-lc-rs as FIPS backend; no hand-rolled crypto primitives.
- **Unresolved → record as open:** aws-lc-rs FIPS coverage of AES-256-GCM-SIV at implementation time (known unknown; verified at ADR acceptance gate).
- **Rejected alternatives:** plain AES-GCM as default (nonce fragility), rolling our own FIPS module, XChaCha20 in FIPS builds (not validated).

### ADR 020 — L2 envelope encryption and DEK lifecycle
- **In:** client-side envelope encryption as authoritative trust model (SSE-KMS is not shipped); per-slab DEK with intra-slab 64 KiB chunked encryption; DEK cache with 15-minute TTL, LRU, `ZeroizeOnDrop`; encryption above `AlignedBuf`; background re-encryption sweeper for plaintext migration.
- **Out:** cipher choice (→ 019), slab layout (→ 022), KMS vendor integration (implementation detail).
- **Rejected alternatives:** SSE-KMS as primary or overlay, per-record DEKs (KMS fan-out), per-whole-slab encryption (breaks range reads), encryption below alignment layer (corrupts O_DIRECT invariants).

### ADR 021 — Tenant-aware architecture from day one
- **In:** per-tenant first-class keyspaces (not prefixes); `tenant_id` threaded through schema, identity (cert OID), encryption context, audit record; policy enforced at both data path (fast) and Raft state machine (authoritative); `TenantAdmin` RPC surface; explicit supersession of `plans/ops/multi_tenancy.md` JWT/prefix model with cross-reference per superseded section.
- **Out:** per-tenant topology staging timeline (phases 1-3 are implementation order, belong in `plans/ops/multi_tenancy.md` rewrite).
- **Rejected alternatives:** deferred multi-tenancy, shared keyspace with prefixes, JWT-bound identity, config-file tenant provisioning.

### ADR 022 — L2 slab format and footer-indexed range reads
- **In:** slab layout `[magic | version_header | records | offset_index | footer_checksum | index_pointer]`; one S3 object per slab; single coordinated `ObjectStore` trait break covering `get`, `get_range`, `put`, `put_multipart`, `delete`, `list`, `head`, `copy`.
- **Rejected alternatives:** sidecar manifests, S3 user-metadata indexes, multi-object slab splits.

### ADR 023 — L2 upload lifecycle
- **In:** Raft-authoritative lifecycle state (`Pending → Uploaded → Committed → LocalPurged`) carrying slab_id, target tier, wrapped DEK reference, state transition timestamps, committing leader node_id; non-authoritative local execution ledger carrying multipart UploadId, temp-file path, part-number → ETag, retry counters; recovery rule (replay local ledger, fall back to `ListMultipartUploads`); multipart trigger aligned with `slab_max_bytes`; part size 8-16 MiB clamped to provider floor.
- **Rejected alternatives:** pure-Raft (bloats replicated state with provider artifacts), pure-local (not durable), single fixed 128 MiB part size.

### ADR 024 — L2 orphan cleanup and remote reclaim
- **In:** leader-only reclaim authority, paged LIST join against `tiered_slabs` state, rate-limited deletion, full lifecycle scope (migration cleanup + retention expiry + tombstone reclamation + orphan GC), archival-tier specifics deferred.
- **Rejected alternatives:** follower-side deletes, eventual-consistency reclaim (decisions need freshest state), archival tier in-scope for pilot.

### ADR 025 — Audit event model and tamper-evident hash chain
- **In:** canonical structured audit event schema from day one; `AuditSink` trait with `LocalNdjson`, `Syslog`, `Kafka` (feature-flagged) implementations; per-record `prev_hash = blake3(previous_record_canonical_bytes)`; chain head exposed via dedicated control RPC; periodic signed-root roll-up.
- **Rejected alternatives:** local-file-only, tamper evidence deferred, blockchain-anchored audit (earlier Gemini position, rejected).

### ADR 026 — FIPS 140-3 certification target and module boundary
- **In:** full 140-3 certification endpoint for a named binary/deployment profile; aws-lc-rs linkage as interim module-only state; cryptographic module boundary documented; entropy-source statement; no non-validated primitives in the FIPS build; CMVP engagement timeline as separate lab workstream.
- **Rejected alternatives:** module-only as finish line, Level 4 hardware module, self-certification.

### ADR 027 — Observability posture
- **In:** OTLP-only trace export; `tracing-subscriber` stack with `EnvFilter` + reload handle + fmt layer (JSON/TTY) + OTLP layer from day one; separate unauthenticated `/health` (liveness) and mTLS-authenticated `/ready` (cluster state); separate listener ports for health and metrics; authenticated OpenMetrics scrape endpoint; trace context propagation across QUIC RPCs.
- **Rejected alternatives:** Jaeger/Zipkin native protocols, shared health+metrics endpoint, unauthenticated metrics.

### ADR 028 — Deterministic simulator and `AsyncFileOps` abstraction
- **In:** dual harness (process-level chaos + deterministic simulator); turmoil-style network substitution under `quinn` plus custom storage simulator; `AsyncFileOps` trait extraction behind which io_uring production path and simulator both implement; mocked `ObjectStore` for tiering; Elle for linearizability subset, monotonic-counter checker for broad coverage; CI/nightly/weekly coverage targets (10k / 1M / 100M operations respectively).
- **Unresolved → record as open:** io_uring refactor cost estimate (2-6 week band, prototype before committing).
- **Rejected alternatives:** custom deterministic harness built directly around `quinn` internals, LocalStack as authoritative S3 test path, monotonic-counter-only correctness checking.

---

## Handling of existing ADRs

| Existing ADR | Action | New ADRs involved |
|--------------|--------|-------------------|
| 006 Tiered storage and lazy migration | Amend — add "Extended by 022-024" cross-link; core framing stands | 022, 023, 024 |
| 007 Raft implementation strategy | Amend — add "Extended by 015" cross-link; core decision (openraft) reaffirmed | 015 |
| 008 Dependency selection | Amend if redb/aws-lc-rs/rcgen aren't already listed | 017, 019 |
| 009 io_uring threading model | Amend — note `AsyncFileOps` trait extraction in 028 | 028 |
| 010 Serialization tiers | Unchanged |
| 011 Auto adaptive transport | Unchanged |
| 012 Bounded clock drift | Unchanged |
| 013 Time travel queries | Unchanged |
| 014 rustfs test backend | Amend if rustfs is retained alongside mocked `ObjectStore` in 028 | 028 |

No full supersessions of existing ADRs. All interactions are additive.

---

## Template addenda

The existing `plans/adr/TEMPLATE.md` has Status, Context, Decision, Rationale, Consequences. Two additions recommended for these ADRs:

1. **Answer-file source** — a single line at the top of Context: "Derived from `plans/gap/<answer-file>.md` §N." Makes the paper trail explicit without bloating the document.
2. **Open questions** — a terminal section listing items the ADR explicitly defers to measurement (e.g., exact group-commit constants, aws-lc-rs SIV coverage). Distinct from "Consequences" because these are *known unknowns to be closed by benchmark*, not accepted trade-offs.

Proposed: add these as recommended sections in `plans/adr/TEMPLATE.md` rather than modifying the required structure. Existing ADRs don't need back-fill.

---

## Schedule

Working days, not calendar days. Assumes one lead drafting plus two parallel reviewers.

- **Wave 1** (ADRs 015-017): draft 3 days, review 2 days, revisions 1 day → 6 working days.
- **Wave 2** (ADRs 018-020): draft 4 days (020 is substantive), review 2 days, revisions 1 day → 7 working days.
- **Wave 3** (ADR 021 + `plans/ops/multi_tenancy.md` rewrite): draft 3 days, review 2 days, revisions 1 day → 6 working days.
- **Wave 4** (ADRs 022-024): draft 4 days, review 2 days, revisions 1 day → 7 working days.
- **Wave 5** (ADRs 025-028): draft 4 days parallel, review 2 days, revisions 1 day → 7 working days.

Total: **~33 working days end-to-end**, or ~6.5 calendar weeks with no slack. Add 25% buffer for review cycles that exceed one revision → **~8 calendar weeks** realistic.

Parallelism is real but bounded — Waves 2 and 3 have a hard sequential dependency (018 → 021), as do Waves 2 and 4 (020 → 022/023/024). Wave 5 can begin drafting as soon as Wave 2 is accepted.

---

## Risks

1. **ADR 019 blocked on aws-lc-rs SIV verification.** If SIV is not yet in the chosen aws-lc-rs release, the FIPS-and-non-SIV branch of the decision tree becomes the hot path, not the fallback. Mitigation: verify *before* Wave 2 starts; if blocked, promote the verification task ahead of the ADR.
2. **ADR 021 scope creep.** Multi-tenancy touches placement, policy, crypto, audit, and observability. The ADR must stay at the architectural level; the tempting per-feature detail belongs in the `plans/ops/multi_tenancy.md` rewrite. Mitigation: enforce 2-page ADR limit, push specifics downstream.
3. **Wave 4 (L2) is dense.** Three interdependent ADRs that all touch the state machine, the object store, and the crypto context. Mitigation: prototype the slab footer format during Wave 2 so 022 has a concrete object to review, not a speculative one.
4. **Reviewer saturation.** Codex and Gemini review every ADR; 14 ADRs at 2-3 reviews each is 28-42 reviews. Mitigation: bundle Wave 5's four parallel ADRs into one review session; keep each ADR ≤ 2 pages.

---

## Exit criteria for this plan

This plan is done when:

- Codex and Gemini have reviewed it and either approved or produced an issues file against it.
- Wave 1 drafting starts.
- `plans/adr/TEMPLATE.md` has the two proposed addenda (or a rejection with rationale).

Not needed: an explicit sign-off document. The `Accepted` status on the first Wave 1 ADR is the execution signal.
