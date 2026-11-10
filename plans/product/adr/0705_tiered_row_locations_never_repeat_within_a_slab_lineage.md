# ADR 0705: tiered row locations never repeat within a slab's lineage

Status: proposed, 2026-11-09 (Claude, Codex). Implementation lands with this plan. Closes DC-195 (registered by ADR 0704) when its implementation and evidence are reviewed. Design cleared over draft r1 and addenda 1–2 (Codex 01a1c3ca, 01a1c3d2, 01a1c3d3). Review record: `reviews/dc195-layout-collision/`.

## Why
- **DC-195 (liveness; fails closed):** a tiered rewrite (ordinary retention, table drop, ADR 0704's certified rewrite) writes its replacement object with offsets restarting at 0. `RecordLocation` equality is (slab, offset, length, hlc, record_row_index, is_tombstone). Rows of one parent write share an HLC, and single-row frames keep their byte length, so when a row is pruned the next survivor slides into its slot and takes its exact location. `TierLayout` refuses the rewrite with `StaleLocation{LayoutCollision}`, correctly: a tiered read verifies only HLC and tenant, so a holder of the old location would silently read the survivor. Such a slab is never cleaned. Both arms reproduced (`a_certified_rewrite_whose_survivor_takes_a_pruned_rows_location_is_refused`, `a_retention_rewrite_whose_survivor_takes_a_pruned_rows_location_is_refused`).
- **The same root, history-dependent (reproduced on main ab3a0628d):** the lineage the collision rule checks (`TierLayout.redirects`/`superseded`) is in memory, per node and per process. The same committed replacement is refused `LayoutCollision` by a node that kept its history and accepted by one restarted between the rewrites. Lineage-cap and history-budget refusals are separate and stay.

## Decision
**Tiered offsets are logical and monotonic across a slab's lineage. A replacement that changes row layout starts at its source object's full aligned logical end; a byte-preserving successor keeps its source's base only when its whole plaintext is proved equal. The object carries its logical base, so the gap costs no bytes. `LayoutCollision` checks stay as defence.**

### 1. The layout type: one checked conversion
`TierObjectLayout { base, plaintext_len }`, constructed only by validating a decoded footer: base page-aligned; `base + plaintext_len` and its page-aligned end checked (an `align_up` overflow refuses even when the sum fits). Only then is `logical_end()` infallible. Its methods are the only logical↔physical conversions:
- `physical_range(&RecordLocation)`: checked `offset − base`, checked end, checked `usize`; a location below `base` or beyond the object refuses typed `StaleLocation{OutsideGeneration}` before any subtraction, range fetch or index lookup;
- `logical_of(physical)`: refuses a physical position outside `[0, plaintext_len)`.

Every range site takes the layout: `extract_record_and_header_from_blob`, the ranged L2 read, `prepare_tiered_range_read`/`verify_adr022_index_contains_range`, `chunk_read_runs_for_plaintext_ranges`, `record_plaintext_range_for_l2`/`_for_crypto`, the blob walks (`walk_slab_records`, `recover_slab_entries_from_blob`, `next_acknowledged_offset`), and local reads of rehydrated copies. Unchecked arithmetic at those sites becomes checked. A logical end is never used as a file or allocation length: temp files and objects stay dense.

### 2. The object format
The ADR 022 footer gains `logical_base: u64` (in place; format bump; old objects refuse typed, pre-release). The footer is integrity-bound to the committed object (`payload_hash` on full fetches, the footer HMAC on ranged reads). The offset index stores logical offsets; every entry must lie within `[base, base + plaintext_len)` (checked) or the object refuses typed `MalformedObjectLayout`, nothing read or published.

### 3. Paths
| Path | Bytes/offsets | Base |
|---|---|---|
| First upload (only from a sealed local slab, `replaces = None`) | preserved | 0 |
| Plaintext→encrypted conversion (only of a base-0 first upload) | preserved | 0 (the source's) |
| DEK rewrap | plaintext preserved (DEK, chunks may change) | the source's, admitted only by the plaintext proof (§4) |
| KEK metadata backfill | same object | unchanged |
| Retention, table-drop and certified rewrites | layout changes | the validated source object's `logical_end()`, from its full plaintext extent (never from surviving or indexed rows); source unavailable → retry/refuse, never 0 |
| Re-tier of a local copy | does not exist | — |
| Rehydrated copy | dense plaintext on disk | held as `RehydratedCopy { store, layout, generation }`; every local read goes through `layout.physical_range`; never selected for another generation (including the readers that bypass the layout today: counter eligibility, LSM counter refusal, the verifier's frame read) |
| Reopen | rehydrated copies are quarantined, not reopened; `tiered_records` hold logical offsets | learned from the current object at first decode |
| Backup / restore | pointers only | carried by the objects |

### 4. The transition classifier
`classify_tier_transition(source, successor)`, called by the leader before proposing and by every node publishing over a locally published source. Both layouts are validated first; the existing current-source (`replaces` == committed) and standing checks run first and stay mandatory. Object preparation stays outside authority leases; the exact source and standing are revalidated at publication.
1. **Same object generation:** validate; the idempotent and metadata-only paths unchanged.
2. **Different object, same base:** admitted only as a proven offset-preserving transform: the successor's complete integrity-checked decoded plaintext equals the source's, byte for byte, bound to these exact two generations. Both are fetched in the prepare phase; a source that cannot be fetched defers or refuses typed.
3. **Layout-changing:** `successor.base >= source.logical_end()`; the leader's builder sets equality to its exact committed source's end.
4. **Anything else:** refused typed `StaleLocation{LayoutRegression}`, nothing published.

### 5. Cold bootstrap and live reconciliation
- **Cold bootstrap** (process start, or a store first populated from a snapshot) trusts the validated current committed object and the producer's transition validation; it does not retrospectively prove the object's ancestry. This is an explicit trust boundary.
- **A live manager** (snapshot install, verifier, reconcile into a running store) keeps the existing publication, history and fencing protocol unchanged; where it holds a published local source, the classifier runs against it. Source absence alone is not treated as "no holders".
- **As built:** the verifier classifies against the local published pointer, which after a restart may be an older durable generation; the existing standing checks still run first, so a divergent local pointer is never taken as an ancestor. By monotonicity any committed successor passes, except a same-base successor whose source object cannot be fetched: it stays deferred. That is an availability dependency on the source object: a transient fetch failure is retried and never resets the base.
- **Cost (as built):** every publication over a different object fetches the full source object in the prepare phase, within the existing allocation and budget bounds. A footer-only fetch would suffice for a layout-changing successor; not done here.

### 6. Stale locations
On every tiered read, in order:
1. in-process history: redirect → target; superseded → `Superseded`;
2. `layout.physical_range`: outside the current object → `StaleLocation{OutsideGeneration}`: "not addressable in this generation" (moved or removed, unknown which), never proof of deletion;
3. inside: the exact-index check, then `verify_frame_names_version` → `Superseded` on mismatch.

`OutsideGeneration` and `Superseded` lead to at most one re-resolution through the index per read; a second stale answer for the same request is a typed refusal, never a retry loop. Only a read that names its version (tenant and key) can re-resolve; an unnamed read's stale answer is final, and its caller re-resolves as it does for `Superseded` today.

### 7. Digests
`lsm_rows_digest` (hence 0704's `postimage_rows_digest`) covers logical offsets; bases are deterministic from committed objects, so digests stay deterministic across nodes.

## Acceptance (finite)
1. Both DC-195 reproducers flip: the original shape rewrites; exact reads before, after and after a real reopen; the survivor's location is at or above the source's logical end; a holder of the pruned location gets `Superseded` in process and `OutsideGeneration` after a reopen, never another row.
2. A three-rewrite lineage on one slab (retention, certified, retention): every issued location distinct; reads exact after each and after a reopen.
3. Unequal history: a node with a longer-lived layout history and a restarted node both accept the same committed replacement (on main, the long-lived node refuses it).
4. Format: a base-0 first upload and a based replacement read identically; the format version test.
5. ADR 0704's workflow and controls pass unchanged.
6. A based object rehydrated and read locally (including through the counter and verifier readers), then a reopen: exact reads; the copy is never selected for another generation.
7. (a) A based encrypted object's DEK rewrap: same base, every location and the plaintext preserved, admitted only by the plaintext proof. (b) A base-0 plaintext first upload's conversion to encrypted: base 0 kept.
8. Bounds: below-base, beyond-end, misaligned base, `base + len` overflow, `align_up` overflow, an index entry outside the extent → typed refusals, nothing read or published.
9. Refused typed at the leader's preflight and at every node's publication: a layout-changing successor whose base overlaps its source; a same-base successor with an equal offset set but a different owner or payload at one offset; a same-base successor whose source cannot be fetched (deferred). Admitted: 7a.

## Not established
- Lineage-cap and history-budget refusals (unchanged).
- Key-verified reads on every path (defence in depth; possibly its own ADR).
- DC-196, §8, follower-held tiered slabs, physical L2 reclamation, the all-member barrier (unchanged from ADR 0704).
