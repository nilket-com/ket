# Workstream D — Audit, Evidence, and Retention

**Merged-plan anchor:** WS6
**Author / Role:** Claude, as Lead Architect on Workstream D
**Version:** v2 — integrates peer review of `codex_workstream_G_20260422.md`. Codex frames the pilot evidence-path as a binary decision (local NDJSON+WORM *or* Kafka-as-evidence) and biases toward the former for first pilot. I hold the more ambitious position (both, with local NDJSON authoritative) but acknowledge Codex's framing is correct for *minimum-viable-pilot*. The two positions are reconciled in §2.6.
**Phase alignment:** Design Phase 0 (after ADR 025 Accepted, Week 4); implementation Phases 1-2.
**Gap items closed:** R-4, R-9, R-10, R-17 (schema piece), R-18, R-19, R-27, R-43.
**Depends on:** WS-A (ADR 025 acceptance). Feeds: WS-B (crypto event emission + `ceremony.executed` event), WS-C (actor identity binding), WS-G (audit-chain integrity preserved across DR), WS-E (FIPS hash primitive), WS-Toron (trace-stitching via `data.read`/`data.write` events with trace_id).

---

## 1. Why this workstream exists

The audit architecture in ADR 025 is genuinely strong: canonical structured events, per-node hash chain with signed 60s roll-up, two authorities (RaftLog vs NodeLocal), fail-closed on NodeLocal writes. What it does not do is cover the three things a regulated Fortune-500 buyer will immediately ask for:

1. **Data-access audit.** HIPAA §164.312(b) and PCI-DSS §10.2.1 require logging every data access. ADR 025's operation enum is closed and contains only admin/auth events. An operator reading tenant data leaves no audit trail. This is the single most visible gap in the audit model; procurement reviewers find it in the first five minutes.
2. **Tamper-resistance beyond tamper-evidence.** Hash chains prove "nobody tampered without detection"; regulators often require "nobody can tamper in the first place." That means WORM / Object Lock storage, not just cryptographic chaining.
3. **Off-box SIEM integration that is actually equivalent.** Today Kafka ships a reduced shape; an external SIEM cannot independently verify the chain. Syslog ships an even smaller shape. Local NDJSON is the only authoritative archive, and its default 90-day retention is two orders of magnitude off the 7-year bar SOX/HIPAA expect.

This workstream closes all three, plus three smaller items (break-glass parameter schema, trace correlation, GDPR right-to-erasure).

## 2. Strategic position (opinionated)

### 2.1 Data-access audit is structural, not sampled-by-default

**Position:** ship `data.read`, `data.write`, and `data.range_query` event types in the audit model. Default sampling policy:

- **Writes:** 100% logged. Writes are the load-bearing events; a missed write-audit is a missed compliance finding. The volume is bounded by ingress throughput, which we already limit.
- **Reads (regulated tenant):** 100% logged.
- **Reads (non-regulated tenant):** 10% sampled by default, configurable per tenant.
- **Range queries:** 100% logged at query-start, with result-count in the record at query-completion. Range queries are the high-privilege data-access shape and should never be sampled out.
- **Cross-tenant-principal reads (operator cert, tenant_id=null):** 100% regardless of tenant tier.

**Why this shape and not "everything 100%, forever."**

Full 100% audit on all reads on a non-regulated-tenant cluster would produce audit volume roughly equal to read throughput. For a busy non-regulated workload (e.g., an internal-use analytical read-mostly tenant), that is a 10x storage-cost multiplier on a workload that does not need the audit evidence. The tenant-compliance-tier config (a new field on the tenant record) selects the policy.

**Why not "100% on writes only, no reads."**

Because data-*read* audit is what most compliance frameworks actually name. A writer can be traced via the Raft log even without audit. A reader who leaves no trace leaves no trace.

### 2.2 Kafka is the default off-box sink; payload is canonical

**Position:** promote the `audit-kafka` feature to the default build. Kafka payload carries every chain field. SIEM can verify the chain independently from Kafka.

**Why this and not "local NDJSON is the source of truth; customer ships it themselves."**

Every regulated Fortune-500 customer asks for SIEM integration. Today they have three bad options: (a) ship local NDJSON via filebeat or similar (works but adds an integration the customer maintains), (b) rebuild Wendelon with `--features audit-kafka` to get Kafka (requires a Rust toolchain the customer's ops team does not have), (c) accept Syslog with reduced fields (often insufficient for the customer's SIEM schema).

Default-on Kafka plus canonical-payload eliminates (b) and (c). It costs ~4 MB in the default binary dependency graph. This is the single cheapest integration we can ship; stop gating it on a feature flag.

**Canonical payload.** Every chain field present: `sequence_number`, `prev_hash`, `record_hash`, `hash_algorithm_tag`, `schema_version`. An off-box chain-verifier can now independently compute `record_hash` and cross-check against the local chain head.

### 2.3 WORM sink is a first-class AuditSink for regulated SKU

**Position:** `AuditSink::ObjectLockWORM` writes to S3 Object Lock / Azure Immutable Blob / GCS Bucket Lock as a first-class sink, not a customer-engineered solution.

**Why.** Tamper-evidence (hash chain) and tamper-resistance (WORM) are different properties. Hash chains catch tampering; WORM prevents it. A regulated customer's auditor will ask for both. We already have hash chains; shipping WORM closes the second half.

**Object Lock mode: governance, not compliance.** The S3 distinction matters. *Compliance mode* is immutable even against the account root; nothing — not legal hold, not ops error — can delete a record before its retention period. *Governance mode* is immutable against normal users but can be overridden by a privileged role (typically for legal-hold adjustments). I recommend governance mode as default; regulated customers can opt into compliance mode per-tenant-tier if their compliance framework specifically requires immutability-even-against-vendor.

**Why governance and not compliance by default.** Real-world legal-hold scenarios (discovery, court-ordered data handling) sometimes need to modify retention on pre-existing records. Compliance mode makes those scenarios impossible and forces operators into ugly workarounds. Governance mode is sufficient for SOX/HIPAA/PCI and preserves legal-hold flexibility. The ability to set compliance mode is present for customers who demand it.

### 2.4 Retention default is 2555 days (7 years), not 90

**Position:** change the default retention to 2555 days. Add a per-tenant retention floor keyed on `compliance_tier`.

**Why.**

- SOX: 7 years.
- HIPAA: 6 years (we round up).
- PCI-DSS: 1 year minimum, 3 years typical, 7 years for investigations.
- Financial regulators (FINRA, SEC): typically 5-7 years.

Today's 90-day default matches no regulatory framework. It matches engineering-team debug-log intuition. Default for a storage product sold to regulated industries should match the industry; the 90-day default is the single change to this ADR that I feel most strongly about.

**Per-tenant retention floor.** `compliance_tier` values: `none` (90-day default), `financial` (2555d), `healthcare` (2555d), `defense` (3650d/10y), `bespoke` (customer-configured, minimum enforced). State machine rejects retention-reducing config changes that would drop a tenant below its tier's floor.

### 2.5 Pilot evidence posture: Codex's minimum is a subset of my full position

Codex's WS-G frames this cleanly: pick *one* evidence posture — local NDJSON with off-box archival, OR Kafka-elevated-to-evidence-grade. Codex's bias is the former for first pilot; promote Kafka only if committing to full chain semantics there.

I hold a stronger position — ship both, with local NDJSON authoritative and Kafka canonicalized — for regulated GA. But Codex is right that the minimum-viable-pilot path is simpler. Reconciliation:

**Pilot phase (Phase 1 exit):** Codex's minimum. Local NDJSON + WORM archive is the authoritative evidence path. Kafka shipping starts but Kafka is labeled "distribution, not evidence-grade" pending its canonicalization work.

**Regulated GA (Phase 2 exit):** Kafka canonicalization complete. Kafka is evidence-grade with full chain fields. Either sink can serve as authoritative evidence; local NDJSON remains the always-on archive.

This preserves both ambitions: we don't force customers to stand up Kafka for pilot, and we don't forever mark Kafka as a compliance second-class citizen.

**Documentation contract.** During the pilot window, `ops/audit.md` must be explicit: "Kafka is a distribution sink; compliance evidence is local NDJSON plus WORM archive." After Phase-2 canonicalization, `ops/audit.md` updates to: "Either Kafka (canonical) or local NDJSON + WORM is acceptable as evidence; confirm with your auditor which they prefer." Contradictory marketing is not acceptable.

### 2.6 `audit verify` CLI is a first-class tool for auditors

**Position:** ship `wendelon-ctl audit verify` that replays a local chain, cross-correlates against peer signed roots, and produces a signed attestation suitable for auditor evidence.

**Why.** A hash chain without a verifier is a hash chain nobody has read. The customer's external auditor doesn't write a Rust program; they run a CLI and expect a PDF-equivalent output they can attach to their audit report. We ship that CLI.

**Output shape.** Signed JSON attestation:
```json
{
  "cluster_id": "...",
  "verified_by": "wendelon-ctl v0.4.0",
  "verification_started_at_ns": 1735689600000000000,
  "verification_completed_at_ns": 1735689660000000000,
  "node_chains_verified": [
    {"node_id": "n-01", "first_seq": 1, "last_seq": 9342,
     "first_record_hash": "blake3:...", "last_record_hash": "blake3:...",
     "signed_roots_verified": 1556, "signed_roots_failed": 0},
    ...
  ],
  "cross_node_anchors_verified": 1556,
  "cross_node_anchors_failed": 0,
  "issues": [],
  "attestation_signature": "ed25519:..."
}
```

The attestation is signed by the `compliance_auditor` role's cert. The auditor's own tooling verifies the attestation signature to establish it was produced by an authorized auditor, not an arbitrary party.

## 3. Design — the components

### 3.1 Audit event model v2 (schema_version = 2)

Additive over ADR 025 v1. New event types:

**`data.read`**
```cbor
{
  "event_id": ULID,
  "timestamp_ns": u64,
  "sequence_number": u64,
  "schema_version": 2,
  "node_id": string,
  "tenant_id": string,
  "actor": {...},               // existing ADR 025 actor shape
  "operation": "data.read",
  "outcome": "Allowed" | "Denied",
  "authority": "NodeLocal",
  "raft_log_index": null,
  "prev_hash": bytes,
  "parameters": {
    "record_identity": bytes,   // 32-byte BLAKE3 Identity (or null for range queries)
    "byte_count_returned": u64,
    "result_count": u32,
    "query_shape": "point" | "range" | "scan",
    "range_start_identity": bytes | null,
    "range_end_identity": bytes | null
  }
}
```

**`data.write`** — parallel structure, `parameters.byte_count_written`, `parameters.slab_id`.

**`data.range_query`** — emitted at query-start with `query_id`; a paired `data.range_query_complete` at completion.

**Sampling policy** (new top-level configuration):

```yaml
audit:
  sampling:
    by_compliance_tier:
      none:
        data.read: 0.1
        data.write: 1.0
        data.range_query: 1.0
      financial: { data.read: 1.0, data.write: 1.0, data.range_query: 1.0 }
      healthcare: { data.read: 1.0, data.write: 1.0, data.range_query: 1.0 }
      defense:    { data.read: 1.0, data.write: 1.0, data.range_query: 1.0 }
    cross_tenant_principal_override: 1.0  # always 100% for cross-tenant reads
```

Sampling decision is pseudo-random and deterministic per (tenant_id, event_id) pair so retries of the same operation produce consistent sampling.

### 3.2 Kafka canonical payload

```json
{
  "event_id": "01HK...",
  "timestamp_ns": 1735689600000000000,
  "sequence_number": 9342,
  "schema_version": 2,
  "hash_algorithm_tag": "blake3" | "sha256",
  "prev_hash": "blake3:...",
  "record_hash": "blake3:...",
  "node_id": "n-01",
  "tenant_id": "tnt-...",
  "actor": {...},
  "operation": "data.write",
  "outcome": "Allowed",
  "authority": "NodeLocal",
  "raft_log_index": null,
  "parameters": {...},
  "break_glass": false,
  "trace_id": "...",              // optional, ADR 025 Open Question resolution
  "canonical_cbor_bytes_b64": "..."  // for off-box chain verification
}
```

The `canonical_cbor_bytes_b64` is the canonical-CBOR bytes of the event (the same bytes hashed to produce `record_hash`). An off-box verifier recomputes `blake3(canonical_cbor_bytes)` and checks against `record_hash`. This is what makes Kafka a compliance-capable sink, not just a distribution sink.

**Topic naming.** `wendelon.audit.v2`. Version in the topic name so a v3 schema ships to a new topic without migrating subscribers.

**Partitioning.** By `node_id`. This keeps per-node chain ordering within a partition for consumers that want to materialize the chain.

### 3.3 `AuditSink::ObjectLockWORM` architecture

**Components.**

1. **Rotator.** Accumulates events in a local buffer; rotates at size (64 MiB) or time (1 hour) whichever first. On rotation, emits a WORM object with chain-continuity metadata.

2. **Chain continuity.** The first record in a new WORM object carries `prev_hash` equal to `record_hash` of the last record in the previous WORM object. Continuity verifiable across WORM objects.

3. **Object naming.** `s3://bucket/cluster-id/node-id/year=Y/month=M/day=D/audit-<start_seq>-<end_seq>.cbor`. Partitioned for S3 list-performance.

4. **Object Lock configuration.** Governance mode (default) or compliance mode (per tenant-compliance-tier config). Retention period matches the tenant-compliance-tier floor.

5. **Signed manifest.** Every 1000 objects, a manifest object written: lists objects with their start/end sequences and record hashes. Signed by the node's signed-root key (ADR 025 §4). Auditors can replay the manifest chain without fetching every object.

6. **Failure semantics.** WORM sink failure does not block the emitting RPC (off-box sinks never block, per ADR 025 §7). The failure is audited on next successful emission. A sink-failure-rate alert pages when WORM upload failures persist.

### 3.4 `wendelon-ctl audit verify` CLI

**Invocation:**
```
wendelon-ctl audit verify \
  --cluster-domain wendelon-prod.example.com \
  --cert ~/certs/compliance_auditor.crt \
  --key ~/certs/compliance_auditor.key \
  --from-ts 2026-01-01T00:00:00Z \
  --to-ts 2026-04-01T00:00:00Z \
  --output attestation.json
```

**Flow.**
1. Connects to every cluster node via mTLS (auditor's cert must be `compliance_auditor` role).
2. Calls `AuditChainHead.get()` on each node; captures current head.
3. Streams the audit tail from each node for the time range (new RPC: `AuditTail.stream(from_ts, to_ts)`).
4. Verifies each node's chain continuity: for each event, `record_hash == blake3(canonical_cbor(event))` and `event.prev_hash == previous_record.record_hash`.
5. Verifies signed roots: every signed-root event's signature is valid against the node's public key.
6. Cross-correlates roots: a signed root from node X at sequence N is visible (as a chained event) on nodes where replication reached that point.
7. Produces attestation JSON; signs with auditor's cert key.

**Output interpretation.** Empty `issues` array means the chain verifies. A non-empty array lists specific chain failures with event IDs and node IDs. An auditor attaches this attestation to their report as tamper-evidence evidence.

**Red-team adversary model.** The CLI does not trust any node individually. If one node's chain has been rewritten, the cross-node anchor verification (signed roots from other nodes) catches the rewrite. The auditor's attestation notes the divergence.

### 3.5 GDPR right-to-erasure

**Position (crypto-shredding only for pilot; documented caveats).** Tenant deletion destroys the Tenant KEK. Slabs for that tenant remain in L2 but are undecryptable. This is crypto-shredding: the bytes exist but the key does not, and the data is effectively erased.

**Caveats (documented in `ops/gdpr_erasure.md`):**
1. Audit records beyond the tenant's deletion still reference `tenant_id` as a string. These are not "personal data" in most jurisdictions (tenant-id is a UUID), but a customer whose tenant-id maps to a named organization in an external registry may need audit-pseudonymization beyond crypto-shredding.
2. Backup copies of tenant-encrypted slabs (in L2 and DR regions) remain undecryptable post-KEK-destruction. Confirms crypto-shredding; no action.
3. Time-travel queries (ADR 013) that reference pre-deletion state are blocked by KEK destruction. No action.

**Pseudonymization deferred.** Mapping `tenant_id` to a pseudonym in historical audit records is a larger project involving legal review. Pilot customers are told: "crypto-shredding meets the letter of Article 17 for the payload; pseudonymization of audit references is a Phase-3 design item."

### 3.6 Break-glass parameter schema (resolves ADR 025 Open Question)

```cbor
{
  ...                              // standard event fields
  "operation": "break_glass.used",
  "parameters": {
    "invoking_cert_serial": string,
    "incident_ticket_id": string,
    "reason": string,             // non-empty, length-bounded [16, 1024]
    "break_glass_scope": string,   // one of: diagnostic, tenant_ops, cluster_ops, key_ops, full
    "cosigner_cert_serial": string,
    "operator_human_binding": string | null  // optional IdP-identity from cert-service
  }
}
```

Schema validated at audit emission time. Missing required fields fail the audit write (and, given NodeLocal fail-closed, fail the operation).

### 3.7 Trace correlation (resolves ADR 027 Open Question)

Add optional `trace_id` field (32 hex bytes, W3C Trace ID format) to every audit record. Populated when the emitting context has a trace attached; null otherwise. Auditor correlates by pivoting from audit record to trace in the observability stack.

## 4. Work plan (Phase-aligned)

### Phase 0 (Weeks 4-6, after ADR 025 Accepted)

| Week | Deliverable |
|---|---|
| 4 | ADR 025 accepted with v2 schema, break-glass parameter schema, trace_id field, signed-root separate-key decision (for regulated SKU). |
| 5 | `AuditSink::ObjectLockWORM` design doc. |
| 5 | Sampling policy config schema. |
| 6 | Kafka canonical payload frozen. |

### Phase 1 (Months 2-10)

| Month | Deliverable |
|---|---|
| 2 | `data.read`, `data.write`, `data.range_query` event types lands. Dispatcher emission wired. |
| 2 | Sampling policy evaluated per event at emission. |
| 3 | `audit-kafka` promoted to default build. Canonical payload shipped. |
| 3 | Retention default changed to 2555d. Per-tenant compliance_tier config. |
| 4 | `AuditSink::ObjectLockWORM` lands. S3 integration tested. |
| 5 | Azure Immutable Blob provider lands. |
| 5 | GCS Bucket-Lock provider lands. |
| 6 | `wendelon-ctl audit verify` CLI ships. Signed attestation output format frozen. |
| 6 | Break-glass parameter schema enforcement. |
| 7 | Trace-id correlation integrated with observability stack. |
| 8 | `ops/gdpr_erasure.md` documented; legal review. |
| 9 | Audit verification demo against a live test cluster; attestation output reviewed by a sample auditor. |
| 10 | Phase 1 exit: all event types shipping; WORM sink operational; Kafka canonical payload live; retention defaults changed. |

### Phase 2 (Months 10-15)

Audit-driven refinements (feedback from first regulated customer), schema v3 planning if needed, pseudonymization design for GDPR Phase 2.

## 5. Interfaces with other workstreams

- **WS-A (Governance):** ADR 025 acceptance (Week 4); break-glass parameter schema resolution.
- **WS-B (Crypto):** crypto events (`crypto.startup`, `crypto.tenant_kek_rotated`, `crypto.tenant_kek_retired`, `crypto.decrypt_rejected`) use the v2 schema. BYOK events if BYOK ships.
- **WS-C (Identity):** actor identity binding (already sourced from mTLS peer). `compliance_auditor` role enables audit-tail-read and `AuditTail.stream` RPC.
- **WS-G (Gemini's WS7):** DR restore must preserve audit chain continuity across a cluster-rebuild event. Manifest design in §3.3 must round-trip through DR.
- **WS-E (FIPS):** hash primitive switches blake3 → SHA-256 under FIPS build (ADR 026 amendment of ADR 025). `hash_algorithm_tag` already in the event shape; consumers handle both.
- **WS2 (Codex's Observability):** metric `wendelon_audit_emit_total{authority, outcome}` already exists; add labels for new event types.

## 6. Exit criteria

1. Audit event schema v2 shipping with `data.read`, `data.write`, `data.range_query`.
2. Sampling policy config honored; verification test: at 10% read sampling, 100% of cross-tenant-principal reads still emit.
3. `audit-kafka` in default build; canonical payload round-trips through an off-box chain-verifier.
4. At least one WORM sink (S3 Object Lock) shipping; integration test verifies chain continuity across rotation.
5. Retention default is 2555 days; per-tenant compliance_tier config enforced.
6. `wendelon-ctl audit verify` produces signed attestation against a live 3-node cluster.
7. Break-glass parameter schema enforced at emission.
8. Trace-id populated when emitting context has a trace.
9. `ops/gdpr_erasure.md` published; legal review documented.
10. Scorecard row "Metrics completeness" includes audit-emission metrics; green.

## 7. Risks within this workstream

- **Audit volume explosion.** 100% data-write audit at high throughput could double storage cost. Mitigation: compression on the local NDJSON; Kafka retention configured separately from local retention; monitor `wendelon_audit_emit_rate_events_per_sec`; documented sizing guide in `ops/multi_tenancy.md`.
- **Kafka default build size.** Adding Kafka client to default binary adds ~4 MB. Mitigation: acceptable; alternative (Syslog) is insufficient for regulated customers.
- **WORM sink provider drift.** S3 Object Lock, Azure Immutable Blob, GCS Bucket Lock have different semantics. Mitigation: `AuditSink::WORM` trait; per-provider adapters; common API emphasizes what they all support.
- **CLI signature verification complexity.** Auditor's tooling must verify attestation signatures; this requires auditor-side tooling beyond "run the CLI." Mitigation: ship a reference auditor-side verification script (Python) with the CLI.
- **Compliance mode vs governance mode confusion.** Customers may ask for compliance mode without understanding the "cannot override retention" implication. Mitigation: documentation warnings; per-tenant-tier config explicitly flags compliance mode; architect sign-off on customer compliance-mode enablement.
- **Data-access audit as an attack surface.** An attacker who can inject audit records fills the SIEM with noise and possibly triggers retention sweeps early. Mitigation: audit emission is gated by the dispatcher (only authenticated contexts emit); injected records fail authenticity checks.

## 8. Points of agreement and divergence with Codex's WS-G

**Agreement:**
- Data-access audit is not optional for enterprise posture.
- Explicit evidence-path decision, with code and operator docs stating the same thing.
- Syslog stays thin; it is a compatibility transport, not an evidence archive.
- WORM archival path for long-term retention.
- Audit verification tooling is a shippable deliverable, not a post-hoc script.

**Refinement from Codex:**
- Pilot-first minimum posture (local NDJSON + WORM) now explicitly §2.5. My fuller position (Kafka canonicalized for regulated GA) is preserved in §2.2.
- Operator-facing verification output (Codex's open question: "how do we present verification results under pressure?") — addressed in §3.4 with a human-readable `--summary` mode on the `audit verify` CLI that produces a terminal-friendly green/red report in addition to the signed JSON attestation.

**Consistent position:** Kafka is marketed as evidence only once it carries chain fields; syslog intentionally thin; WORM first-class for regulated.

**Not contested:** everything else.

## 9. Opinionated Constraints (framing adopted from Codex)

- Do not market Kafka as evidence unless it actually carries evidence.
- Do not add data-access audit events and then hide them behind vague sampling defaults.
- Do not leave auditors dependent on local ad hoc parsing and tribal knowledge.
- (Added by Claude:) Do not make retention default so low that enterprise customers need a config change on day one — 7 years is the default, not the upgrade.
- (Added by Claude:) Do not write `data.read` records that omit the result_count; "how much did they see" is the question auditors ask most.

## 10. Open questions

1. **Sampling policy for range queries.** I proposed 100%. A tenant with millions of small range queries a day may push back. Alternative: 100% at query-start, emit `data.range_query_complete` only for queries that returned >N rows. I lean against: the start record is the one that captures intent; skipping completes loses the "how much did they see" signal.

2. **Is `compliance_tier` a static tenant attribute or dynamic?** I proposed static (set at tenant creation). Case for dynamic: a tenant upgrades from non-regulated to regulated, retention floor snaps up. Case for static: avoids retention-policy-applied-retroactively complications. I lean static with a documented workflow for migrating a tenant to a higher tier (involves operator ceremony).

3. **WORM default: governance or compliance mode?** I proposed governance. Some regulatory frameworks (e.g., SEC 17a-4 strict interpretation) demand compliance mode. Default per compliance tier: governance for `financial`, compliance for `defense`? I lean per-tier configurable with governance as fallback.

4. **Signed-root key separation from mTLS node key.** ADR 025 Open Question. I argued for separate key (regulated SKU). Case for shared: one HSM, one ceremony. Case for separate: compromising the mTLS key does not compromise audit. I lean separate. Deferred to WS-C (identity workstream) since the cert-service owns signing keys — want their read.

5. **Kafka topic partitioning.** I proposed by `node_id`. Alternative: by `tenant_id` (groups a tenant's audit together but breaks cross-node chain ordering). Alternative: by event time (simpler but breaks ordering). I lean node_id; tenant-id partitioning would require maintaining per-tenant chain state per-node, which we deliberately don't do.

6. **Retention enforcement at which layer?** The rotation sweep deletes old NDJSON files; the WORM sink's object-lock handles WORM retention. They should agree but don't automatically. Design question: does the retention sweep check the sink's confirmed-archived status before deleting local? I lean yes — don't delete local until WORM confirms archived.

7. **Trace-id in audit: privacy considerations.** A trace-id links an audit record to observability data. If observability data is retained shorter than audit data (typical — metrics 30d, traces 7d), an audit record pointing to an expired trace is stale. Acceptable? I lean yes; trace-id is best-effort correlation.

8. **What about non-English reason strings in break-glass?** `parameters.reason` is a free-text string. Length-bounded to [16, 1024]. Does it support Unicode? (I lean yes, validate UTF-8.) Does it require a pre-specified language? (I lean no.) These small decisions matter for global customers.

9. **Audit archive migration during cluster rebuild.** WS-G's DR plan rebuilds clusters; what happens to the audit chain? The node_id changes on rebuild; chain continuity is per-node-id; post-rebuild the chain "forks." I propose: a `cluster.rebuild` audit event marks the fork point, and verifier tools know to follow the fork. Needs coordination with WS-G.

10. **Pseudonymization for GDPR — when?** I deferred to Phase 3. A design-partner customer in EU jurisdiction may demand it earlier. What's the minimum viable pseudonymization (e.g., hash `tenant_id` with a cluster-internal salt on events older than a threshold) vs. the right pseudonymization (full PII redaction with legal review)? I lean minimum-viable is shippable in Phase 2 but commits us to the simple model; right-thing needs Phase 3 legal engagement.

11. **Operator-facing verification UX (adopted from Codex's open question).** Signed JSON attestation is for auditors. Operators under incident pressure need a terminal-friendly summary. I added a `--summary` mode in §3.4 that produces a green/red report. Is that sufficient, or should we add a dashboard integration (grafana panel showing chain continuity and latest verification result)? I lean on the CLI for pilot; dashboard for GA.

12. **Trace-stitching for Toron-originated audits (cross-ref with Gemini's WS-B).** Gemini's Toron integration proposes Toron injects W3C Trace Context into QUIC headers, which Wendelon attaches to audit `data.read`/`data.write` events. My §3.7 already has `trace_id` field; alignment with Gemini is clean. The remaining question: does the audit record's `trace_id` correlate to Toron's *session* trace or the Toron-to-Wendelon *request* trace? I propose request-trace (W3C Trace Context propagates one trace_id); session-trace stays in Toron's own audit if they keep one.

The first three questions matter most for Phase 1 scope; the rest can be locked in Phase 2 design-lock entries.
