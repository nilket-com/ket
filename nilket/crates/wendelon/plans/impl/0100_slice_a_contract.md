# ADR0100 slice A - Concrete contract before code

Status: REVIEW REQUIRED BEFORE CODE, 2026-10-05. User authorized slice A only via chatd `01a10db7-61b8-74d2-bbe4-6f163efbe5c8`, contingent on review of this document. Isolated branch `adr0100-slice-a`, base signed selection `f807ee6e`. Revision r2 answers review `01a10dc0-d4a9-713e-8f8d-8d33ad282791` (K1/K2 and N1–N3). This draft freezes proposed encodings/bounds for review; it does not itself activate a live storage format or wire a production secret. No main FF, GitHub push, staging or host campaign. B/C and all ADR629 holds unchanged.

## Delivery scope

Implement a library substrate: scoped identities, opaque keyed commitments, bounded manifest codec, immutable decision authority and its deterministic candidate snapshot/replay codec. No production Finalize/request path, boot/key loader, logical reader, fresh-cut RPC or epoch/refusal policy is wired to it in A. The authority accepts trusted state-machine input; it is not a new unauthenticated admission API. Existing store visibility remains broken and the characterization stays intact.

A's snapshot/replay equality means the new authority's exact dormant representation and deterministic event replay, not an already-integrated daemon restart or live WAL5/snapshot6. Integration/old-format refusal across every ingress is B/stage3 work before activation. Slice A must not be presented as that coverage. Current WAL4/snapshot5 constants and current protocol remain unchanged on this isolated substrate build.

Proposed source module: `wendelon/src/storage/mutation_visibility/` with separate identity, commitment, manifest, decision and codec modules, re-exported through storage. Ordinary library types compile; the only implemented secret provider is cfg(test). Existing RootKey/TLS/lease constructors are not reused. No source size cap changes intended; architecture prechecks cover the new modules explicitly.

## 1. Fixed identity encoding

All integers unsigned little endian; arrays raw fixed bytes; no host struct padding, postcard varint dependence, map iteration order or string enum names. Unknown versions/tags, trailing bytes, arithmetic overflow and truncation refuse. Canonical identity is 73 bytes:

| Offset | Width | Field |
| --- | --- | --- |
| 0 | 16 | StoreGeneration (opaque UUID bytes, nonzero) |
| 16 | 8 | tenant_id (zero allowed only where the existing namespace explicitly permits it; not a privilege grant) |
| 24 | 1 | Scope: 0 Raw, 1 Table |
| 25 | 16 | table_object_id; Raw exactly zero, Table nonzero |
| 41 | 8 | schema_generation; Raw exactly zero, Table positive |
| 49 | 16 | caller client_uuid, nonzero |
| 65 | 8 | caller sequence_no |

Caller identity follows `ClientWriteId::canonical_idempotency_bytes` (`schema/core.rs:173`): UUID + sequence only. `idempotency_ttl_ms` and `_pad` are hints/alignment and EXCLUDED. Different TTL/pad retries therefore share identity; noncanonical wire padding remains the existing wire-validation issue. No equality based on UUID alone, HLC alone or an unscoped pending hash.

`Admission` is epoch u64 positive + holder u32 positive (12 bytes). It is immutable provenance, outside the stable caller identity. Exact replay uses the original admission; changing epoch/holder while keeping parent identity/content is a conflict, not re-admission. Handling a client's retry whose data path automatically acquires a new admission is a stage3 integration gate, not silently allowed here.

`ProofKeyBinding` is generation16 + tenant8 + key_id16 nonzero + key_version u32 positive (44 bytes). Manifest already contains generation/tenant; its key reference is only key_id16/version4. No raw secret is serializable. Key id is a public opaque selector, not the secret itself.

## 2. ProofKey provider and commitment domains

Abstract trait proposal: synchronous `keyed_digest(binding, canonical_preimage: &[u8]) -> Result<[u8;32], ProofKeyUnavailable>`. It borrows bytes for the duration of the call, returns a digest, and never returns a raw key. The provider contract selects one stable secret for the exact generation/tenant/key_id/version. It must reject unavailable/wrong scope/version/not-authorized/backend failure with bounded typed errors, never fall back to another secret or regenerate. No default provider implementation and no insecure default key. Synchronous A trait performs no blocking I/O: future provisioning resolves a protected key beforehand or returns unavailable; an I/O-backed/async provider requires separate design review.

The only implementation in A is cfg(test), using explicitly supplied 32-byte test keys and existing `blake3::Hasher::new_keyed`. Test key material is deliberately synthetic; it proves binding/codec behavior, not deployed confidentiality. Owned test secret buffers use existing zeroize support, never Serialize/Debug/Display/Clone. Do not print keys or canonical customer preimages in errors/traces; DTO Debug exposes only redacted type/scope/count metadata (not raw tokens or digests). No provider/secret in Control Ring messages, snapshots or backups. Operational authenticated provisioning, protected storage/escrow/restore, role separation, retention and rotation remain a separate pre-B runtime gate.

Canonical preimages use these distinct ASCII domain bytes, including the final NUL, then fixed context, then a u32 byte length and the exact borrowed input where present:

- Key token: `wendelon/adr0100/key-token/v1\0` + ProofKeyBinding44 + key namespace (identity bytes0..41: generation16 + tenant8 + scope1 + table_object_id16; excludes schema_generation and caller UUID/sequence) + key_input_codec u16=1 + key_len u32 + canonical key bytes.
- Row content: `wendelon/adr0100/row-content/v1\0` + ProofKeyBinding44 + full MutationIdentity73 + Admission12 + operation u8 + child_ordinal u32 + row_ordinal u32 + key_token32 + tombstone u8 + content_input_codec u16=1 + content_len u32 + canonical content bytes.
- Parent manifest: `wendelon/adr0100/parent-manifest/v1\0` + ProofKeyBinding44 + manifest_len u32 + canonical manifest bytes (without the aggregate commitment).

For each domain the provider computes keyed BLAKE3 over those exact bytes. Key token is stable for the same canonical key bytes within one generation/tenant/scope/table_object_id/key binding, across schema-generation changes; equality/access-pattern leakage is explicit. Content binds parent/child/row and operation, preventing cross-parent/domain substitution; it excludes physical slab offsets/ciphertext packing. Parent commitment includes the full key reference and all ordered children. Output is 32 opaque bytes. Scope/key-version mismatches cannot satisfy the same preimage. These are cryptographic assumptions plus regression pins, not a proof against a compromised provider/trusted writer.

Schema generation remains in the full73-byte mutation identity and row-content/parent-manifest commitments, so those bindings still distinguish schema revisions. It is deliberately absent from the41-byte key-token namespace. This preserves token equality across a schema revision ONLY if the canonical key bytes and key codec/binding remain unchanged. Changes to primary-key definition or canonicalization, table-object-id reuse after drop/recreate, and key rotation require an explicit continuity/incarnation policy at B before activation; A does not infer equality across them. Runtime routing/predecessor integration remains B work; tokens are opaque equality evidence, not physical routing addresses.

Input codec1 in A means explicit, already-canonical logical bytes supplied by a trusted data-plane adapter; A commits to those exact bytes and performs no Arrow/SQL conversion. `CanonicalKeyBytes`/`CanonicalContentBytes` wrapper construction checks length only and makes this trust boundary visible. B must define and independently pin schema/type/null/string/float/timestamp canonicalization before runtime integration; Arrow IPC bytes, compressed bytes or equal HLCs are not assumed canonical logical contents. Changing that conversion requires a new codec tag, never reinterpretation of codec1. Codec1 byte fixtures in A have precise expected commitments; no claim of general logical-row equivalence yet.

Tombstone content input is exactly empty; operation/child/key/tombstone context still binds deletion. A live row content may be empty only if its separately reviewed logical codec permits it; empty input is not a deletion flag. Key input is nonempty. Control manifests contain opaque tokens/commitments only, never these preimages, existing unkeyed Identity, plaintext PK or key-derived locality. Assigned coarse range/partition membership and counts remain sensitive metadata. Existing Finalize key_identity projection is not repaired by A; no new manifest is connected to it.

### Provider failure/restore contract

Generation/tenant/key-version references persist; reopening does not mint keys. A round trip can be decoded without secret material, but recomputing/verifying a content commitment needs the same authorized binding. Missing key yields unavailable, not acceptance or a new token. Tests use a separately constructed provider with the same explicit test key to prove stable restore binding, and wrong/missing providers to prove failures. No production escrow/provisioning implementation is claimed. Keys cannot be discarded while rows/decisions still need them. Future rotation selects a new binding and retains the old one; it must not change an existing parent replay's key reference.

## 3. Manifest shape and limits

Manifest format u16=1; deterministic 112-byte header followed by ordered children. Header:

`version2 | identity73 | admission12 | key_id16 | key_version4 | operation1 | content_codec2 | child_count2`.

Operation tags: 1 Insert, 2 Update, 3 Delete (Table scope); 4 RawPut, 5 RawDelete (Raw scope). Unknown or scope-mismatched tags refuse. Key-input codec is fixed1 by this manifest version; content codec must equal1. Every table child/row is one parent's logical mutation, not arbitrary packed physical membership.

Each child header (31 bytes): `child_ordinal4 | assigned_range_start8 | assigned_range_end8 | partition_present1 | partition_id8 | row_count2`. Ranges are inclusive coarse assigned ranges, start<=end, not a row's key/locality hash; wraparound ranges split into separate children. Partition absent=flag0/id0, present=flag1/id positive. Child ordinals are exactly 0..child_count-1, sorted; nonempty rows. No physical offset or original Identity in this DTO.

Each logical version (69 bytes): `row_ordinal4 | key_token32 | content_commitment32 | tombstone1`. Row ordinals exactly0..row_count-1; tombstone exactly0/1 and consistent with operation. Same key token cannot occur twice anywhere in a parent (duplicate PK/raw key rejects rather than inventing an intra-parent order). A's full accepted manifest covers all listed logical versions; completeness relative to an actual table input is B's adapter/replica pin.

Frozen proposal bounds (safety/working-set choices, NOT performance-derived thresholds):

| Bound | Value | Refusal |
| --- | --- | --- |
| children per parent | 1..256 | typed InvalidManifest/LimitExceeded |
| logical versions per parent, aggregate | 1..512 | same, checked accumulation |
| canonical manifest bytes | <=65536 | reject before allocation |
| canonical key input | 1..4096 bytes | before preimage allocation/provider call |
| canonical content input per logical version | <=1048576 bytes | before preimage allocation/provider call |
| terminal decision records | <=32768 | CapacityUnavailable, no new decision |
| aggregate canonical stored decision bytes (including per-record length prefixes) | <=66846720 (64 MiB minus256 KiB envelope reserve) | CapacityUnavailable, no new decision |
| dormant epoch-cut records | <=4096 | snapshot/codec refuses overflow |

Maximum structural manifest under these limits is 112 + 256*31 + 512*69 = 43376 bytes, below65536. The larger byte ceiling guards decoded/future misuse but permits no new extension under version1. Reject unexpected trailing bytes; don't accept zero padding to65536. Encoded sizes use checked operations; no Vec::with_capacity(untrusted count) before count/byte checks. Input buffers are borrowed; there is no unbounded copy of a million rows. Per-content1MiB is a deliberate A safety bound; rows needing more require reviewed limit changes, not truncation. Existing A1/A2 fixtures are measured against these bounds in the later adapter inventory.

### Finite retention and admission capacity (K1)

Retain the32768-record cap and66846720-byte cap for A; this is a finite characterization substrate, not indefinite store lifetime. A minimum single-row manifest is112+31+69=212 bytes. Its record body is270 bytes and its length-prefixed stored record274 bytes. Thus32768 such decisions consume8978432 bytes (about8.56 MiB), and the count cap binds before the byte cap. At300 new terminal parent decisions/hour, an initially empty authority reaches32768/300=109.2267 hours, about4.5511 days. At100/hour it is327.68 hours, about13.6533 days. These are examples, not capacity guarantees: Refused records consume capacity too, every distinct parent consumes a record, multi-parent requests can consume several, larger manifests hit the byte cap earlier, and pre-existing records/reservations reduce headroom. Retries of an exactly matching parent consume no additional terminal record.

Before any activation or campaign, B/stage3 must compute both bounds from the declared workload and actual initial usage, including all producers, refusals, maximum encoded parent sizes and in-flight reservations. A campaign beyond that computed bound is HELD until a separately reviewed pruning/summary or capacity design preserves the authority needed for replay, visibility and predecessor checks. No eviction, forgotten-decision-as-Unknown, or automatic id reissue is supplied by A. The low-rate multi-day store goal therefore retains this explicit must-close gate; a24-hour example alone does not clear a72-hour or longer campaign.

Integration admission MUST reserve one decision slot and a checked sufficient encoded-byte allowance for every admitted parent BEFORE any durable child append. Reservation and capacity accounting must serialize concurrent admissions; checking only the current terminal count is insufficient. Outstanding/in-flight and unresolved parents retain their reservations until an authoritative terminal transition or a reviewed safe release; a client timeout is not a release. Restart reconstructs a validated capacity/reservation view before admitting writes. If sufficient capacity cannot be reserved, reject with a typed capacity outcome before durable append, not a persisted Refused and not permanent Unknown debt. A implements only terminal-map limits and replay at capacity; durable reservations/reconstruction are B/stage3 obligations and a blocking activation gate.

The512-version bound is also user-visible: a single INSERT exceeding512 logical versions must be rejected at statement admission BEFORE any durable append. The same pre-append rule applies to every manifest/input bound. Bulk callers must explicitly submit bounded statements; there is no transparent chunking that silently replaces one statement's atomicity with several independently acknowledged parents. B inventories MERGE, UPSERT and CTAS as well as INSERT/UPDATE/DELETE and their parent/child mapping. Current SQL UPDATE of the primary key is forbidden (`toron` binder.rs:2866); this does not establish the other entry points' behavior. Any path that can change a key or couple operations must be designed and pinned before integration.

The256KiB reserve covers the bounded dormant cut list and snapshot headers/checksum; capacity checks include the full encoded stored-record length. No decision-map heap bound is inferred from the canonical byte cap. Measure resident/encoded entry sizes and snapshot overhead in A's package; activation/performance budget still reviewed at stage4. Capacity check/replay semantics below are essential: existing decisions remain readable and exact replay succeeds at capacity.

## 4. Immutable decision model

`DecisionRecordV1`: identity, Admission, key reference, complete canonical manifest, its32-byte aggregate commitment, first decision full Raft LogId, terminal kind and typed cause when refused. Full manifest is retained for BOTH states so exact replay/context/content conflicts do not depend on response cache retention. No plain row preimage/secret is stored. First LogId uses term u64, leader node u32 and index u64 (20 bytes), positive term/node/index. Actual committed Raft lineage is supplied by trusted integration, not certified by merely decoding these integers.

States: Accepted or Refused. Unknown is absence only from a validated authority store of the correct generation. Authority unavailable/corrupt is NOT an empty validated map. Refused cause tags frozen for candidate codec: 1 EpochCut, 2 InvalidAdmission, 3 ExplicitPolicy. Canonical stored record body: manifest_len u32 + manifest bytes + aggregate_commitment32 + first_LogId20 + terminal_kind u8 (1 Accepted,2 Refused) + cause u8 (0 only for Accepted,1..3 for Refused). It is exactly manifest_len+58 bytes; the snapshot adds a4-byte record length prefix. All identity/Admission/key-reference fields are the manifest's single authoritative copies, not a second possibly inconsistent header. No string or embedded customer data; these are reserved typed results, not live epoch policy. Unknown tag refuses.

Pure authority operations take a structurally valid manifest/commitment and trusted log coordinate. A verifies structure and stable binding/equality; control-only authority cannot recompute customer commitments without a data-plane secret. Data-node content verification and authenticated quorum claims are integration obligations, not supplied by the MAC's presence. No constructor named “verified customer contents” on control-only decode.

Decision rule:

1. Validate generation, identity/manifest consistency, bounds and coordinate shape. Incoming valid events must follow the model's applied-log order; same-index differing entry is corrupt/conflict, never overwrite.
2. Lookup stable identity. If exact manifest bytes, Admission and key binding/commitment match, return the ORIGINAL decision/index/cause, regardless of a later proposed Accepted/Refused or new Raft log index. No counter/CDC charge; those integrations are not present in A. If binding/content/admission differs, typed Conflict, record unchanged.
3. For absent identity, check decision-count and aggregate-byte capacity BEFORE inserting. If capacity unavailable, return CapacityUnavailable and leave the identity Unknown. This is NOT a persisted Refused and cannot supply a terminal refusal receipt. No evictions. B/stage3 must implement the pre-append reservation gate above; preserving capacity-induced unresolved debt is NOT an acceptable activation policy. A does not claim to implement that admission gate.
4. Otherwise persist the requested terminal decision at its first LogId. Accepted never flips to Refused, Refused never to Accepted. Concurrent callers linearize under one authority lock; no await/I/O/provider call while holding it.

Pure deterministic replay models valid log processing separately from inserting a decision: semantically rejected/conflicting/capacity events may advance applied coverage while leaving decision records unchanged. Invalid/corrupt serialized entries halt decoding before coverage advances. This distinction must be in tests; “record unchanged” does not imply the state-machine applied coordinate never advances. Higher applied coverage never makes an absent parent Accepted.

## 5. Dormant WAL5/snapshot6 choice

Use dedicated `VisibilityAuthorityExtensionV1` codec as the exact dormant authority representation: generation, model applied LogId, last processed event fingerprint, sorted decision map, checked aggregate-byte counters recomputed from records, and `ReservedLeaseFenceFieldsV1`. The fingerprint is an unkeyed BLAKE3 integrity hash of an already-opaque canonical event, not a customer-content commitment or authenticity proof. Equal latest event coordinates require equal fingerprints; earlier indices refuse unordered replay. A caller retry arriving at a NEW higher log index can still return the original terminal decision. Snapshot-tail replay therefore starts strictly after its applied coordinate, not by blindly reapplying arbitrary earlier entries.

Reserved fields: lease rule tag1 `AlwaysMonotoneV1`; epoch rule tag1 `RefusePostCutOldEpochV1`; initial_epoch u64 positive; sorted optional cut history, each prior_epoch u64/new_epoch u64/LogId20. Cut prior<new, chained from initial epoch, increasing LogIds <= model applied; count<=4096. These are format declarations, NOT runtime policy activation or a claim that production epoch cuts are now fenced. A can serialize/validate equality on explicit synthetic cuts; no live AdvanceEpoch/AcquireLease path invokes it.

The extension has a strict envelope: magic8 ASCII `VISAUTH1` + extension_version u16=1 + payload_len u32 + payload + checksum32 (unkeyed BLAKE3 over magic/version/length/payload, integrity only). Entire envelope <=67108864 bytes; the parser checks this before payload allocation. Payload header is92 bytes: generation16 + applied_LogId20 + last_event_fingerprint32 + initial_epoch8 + lease_rule1 + epoch_rule1 + cut_count2 + decision_count4 + stored_record_bytes8. Then cut_count fixed36-byte cuts; then decision_count records, each prefixed by record_len u32 and its exact canonical body. The stored_record_bytes counter is the sum of those length prefixes and bodies. With at most4096 cuts, the256KiB envelope reserve suffices (92+4096*36+46=147594). No generic serde map order: records sorted by the manifest's73-byte identity encoding, unique identities. Extra fields/trailing bytes refuse; counters, generation, key/manifest fields and decision coordinates must agree. Checksum failure refuses before publishing decoded state; checksum is not a substitute for role authorization or commitment verification.

Candidate replay event envelope: magic8 ASCII `VISEVENT` + version u16=1 + payload_len u32<=65536 + payload + checksum32 using the same integrity rule. Payload tag1 Decision is tag1 + event_LogId20 + proposed terminal kind1/cause1 + manifest_len4/manifest bytes + aggregate_commitment32; tag2 Coverage is tag2 + event_LogId20 only. All tags/lengths/order checks are strict. No plaintext/secret in events. Event checksum/fingerprint and model coverage distinguish repeated identical latest events from same-index different proposals; corrupt input never advances coverage. Coverage acknowledges a trusted non-decision committed log step in the model; it is not proof that unknown parents finalized. Actual root-WAL dispatch is not wired by A.

WAL5 and snapshot6 root encodings/activation are DEFERRED: current live versions remain4/5. At B/stage3, compose this frozen extension with the reviewed new root request/SmState shape and strict refusal of older artifacts at every ingress. The eventual v5 Finalize replaces the legacy exposed identity with the opaque projection and carries exact parent binding; its complete request spine is not defined or delivered by A. A supplies candidate decision-event serialization only, not a falsely complete v5 WendelonRequest codec. The eventual v6 snapshot includes authority and reserved fields, plus the reviewed remaining state; A does not publish v6 daemon snapshots yet.

If B's provenance/root format or stage3 policy needs a different extension shape, bump extension version and reserve new root versions as required; never reuse version1/v5/v6 for incompatible encodings. Golden extension bytes remain evidence. Root version numbers are not marked delivered until their complete encoders/decoders/ingress tests are implemented and reviewed. Missing raw-field secrecy or an undecided serializer spine is a HOLD before any activation.

## 6. Proposed pins and package gate

- Canonical73-byte identity goldens; TTL/pad retries equal; tenant/scope/table/schema/client sequence differ. Invalid scope/tags/zero generation/table/node reject. Key-binding and operation/context serialization goldens independent of architecture endianness.
- Keyed-domain byte goldens and expected synthetic key/content/manifest commitments; low-entropy email/key inputs; changes of namespace, key version, parent, child, operation/tombstone and content fail equality appropriately. Same canonical key and binding across a schema-generation bump yield the same key token but distinct full identities/content commitments; cross-tenant/generation/table distinct. Changed key bytes or key binding are not continuous by assumption. Missing/wrong-version/not-authorized provider unavailable; test provider cfg(test) compile-out; Debug/error representations contain no key/preimage/token output.
- Boundary cases children256/257, versions512/513, manifest exact length, max key/content byte length and +1, unchecked overflow/truncated count, duplicate tokens/ordinals/unknown tags/trailing bytes. Decoder rejects before allocation; encoder canonical order is checked, not silently sorts caller ambiguity.
- Accepted -> replay Accepted; Accepted -> later proposed Refused returns first Accepted; Refused -> proposed Accepted returns first Refused; conflicting content/Admission/key version/tenant cannot overwrite; same log index different proposal conflicts. The same caller UUID/sequence under two tenants creates two independent decisions in both insertion orders, including identical logical bytes; this pins tenant scoping at this authority only and does not repair the existing F9 runtime path. Concurrent races in both orders pinned with explicit barriers; serial seeded sequences supplement, not replace concurrency coverage.
- Count/byte capacity small-config tests plus default-limit structural checks; replay at capacity succeeds, new parent Unknown + CapacityUnavailable, no success/refusal receipt, no eviction; independently checked274-byte one-row record and count/byte arithmetic; applied-coordinate versus record-change distinction. Production pre-append reservation/concurrency/restart and statement-limit admission tests are required at B/stage3, not claimed by A.
- Snapshot extension round trip, deterministic ordered encode, zero-counter forgery, duplicate/missing map keys, wrong generation, malformed decision/cut lineage, optional dormant cut history, raw old/current root bytes rejected by extension decoder without modifying them. Replay into fresh authority yields the same exact extension/digest; replay from a snapshot plus tail equals uninterrupted replay. Missing/truncated authority is unavailable; no empty-default reopening.
- Restoration with same explicit test key reproduces commitments; wrong/missing key fails; snapshot contains neither secret nor canonical customer preimages/original routing Identity. This is extension structural evidence, not a claim about every live Control Ring artifact or operational secret backup.

No source begins until this contract's encoding, capacities, provider and deferred-root choice clear review. Then one isolated signed package after full precheck and pinned1.95.0 bigfoot ladder: --no-fail-fast; Wendelon serialized; applicable Wendelon/Toron/tpch/client suites; fmt -p only; no concurrent mutation of build trees. Retain failed/superseded evidence. Measure resident/encoded decision sizes; no host campaign or deployment. Main FF and origin remain held.

## Requested clearance before code

1. Canonical fields/domain preimages and trusted logical-input codec1 boundary (B owns Arrow/runtime conversion) clear?
2. Proposed manifest/input/map bounds clear as conservative safety limits, with no performance claim?
3. Abstract provider plus cfg(test) explicit secrets only; operational key lifecycle still a separate pre-B gate clear?
4. Immutable authority/replay/capacity behavior and trusted-input boundary clear?
5. Dormant exact extension with root WAL5/snapshot6 activation deferred (not falsely delivered) clear? This narrows A to its authorized substrate while preserving stage3 ordering.
