# ADR 0106 - A promoted replacement recovers its slot's data

## Status
Accepted for implementation, 2026-11-16 (member replacement gate 3; scope amended by Codex 01a1e1e4). Codex accepted B1 (01a1e16d), accepted correction r2 with clarifications (01a1e171), and cleared F2's lifecycle (01a1e1b7, after the 01a1e181 and 01a1e1b2 holds). Not delivered. The unit ends at "J has proved its recovery and remains ControlOnly". Activation, the local role change, elections and serving are gate 4.

**Scope amended (Codex 01a1e1e4):** taking part in certification (the ordinary report → commit → activate lifecycle on the restricted store) is split out to ADR 0107, design-first. Until 0107, certified-generation progress stays blocked from promotion onward, as it has been since ADR 0105: a commit needs the promoted store's OWN report, and its `RecoveryProved` is not one. Retained decision data cannot be erased through certification meanwhile, and the existing admission and resource limits still apply.

## Context
- ADR 0105 left slot N with a promoted store J that is current, bound to the slot's key, empty of data, and ControlOnly.
- **No path copies a certified base.** Base entries are metadata, and their rows live in slabs (certified_base.rs:195-204). Certification has "no reset or copy" (certification.rs:8-9).
- **Child repair refuses J at both ends**, because recorded plans name (N, I) (child_writes.rs:305-311; repair.rs:44-99).
- **Certification cannot be the coverage barrier unchanged.** An idle cluster never proposes a target. Equal reports prove metadata. A certificate keeps no member list (generations.rs:64-72, 335-367).
- **The certificate is fixed while J recovers.** `commit_generation` needs a report from every current member, and the members include (N, J) (generations.rs:250-266).
- **Trust model (unchanged):** trusted nodes, crash and stale-state faults.

## Decision

### D1. Authority
Recovery serves only the CURRENT replacement of slot N:
- the applied state holds a Done replacement with token T, a non-retired serving r_new and birth B;
- its promotion recorded J;
- `incarnations[N].current == J`;
- (N, J) is bound to the slot's key.

Every recovery step rechecks this, and a failure stops it for good, as in ADR 0105 R2. An old Done record or an old J authorizes nothing.

### D2. Inventory, at one applied cut K
- **The certified base at G,** the current certificate's gid. G = 0 is genesis: no certificate, an empty base.
- **Every child whose body is `Accepted { dec }` with dec ≤ K** and whose plan names an ancestor-or-self of J (D4).
- **Delta:** every such child with dec > K, whatever its rsv, processed in dec order. `delta_through_dec` advances only over a fully processed interval; an eligible child is never skipped.
- **Not copied:** replicated records, history and pointers (J already holds them), superseded versions (PD refuses retention floors), I's log and I's objects.

### D3. `TABLE_BASE_FETCH`, the one new interface
- **Request:** `{token, r_new, birth, store: J, gid, target_digest, chunk, after: Option<(tenant, table, key_identity)>, max_rows ≤ 1024, max_bytes ≤ 4 MiB}`.
- **The source admits it only when:**
  - the authenticated slot is N, and (T, r_new, B, J) is exactly the applied current replacement per D1;
  - the source is a member other than N, with `active_gid == gid`, validated and not behind;
  - the current certificate is (gid, target_digest).
- **Otherwise** a typed refusal: `NotAuthorized` / `WrongGeneration` / `NotReady`.
- **Answer:** entries in key order with position; the rows at exactly those versions, read through to the source's own L2 when cold; tombstones typed; the next cursor; the chunk count on the last page.
- **Receiver checks on every page:** keys ascend; rows match their digests; positions are recomputed; the page bounds hold. At the chunk's end: the count and the sums equal the certificate.
- **Failure.** A bad page or chunk is discarded and the next source is tried. With no source, recovery waits and reports it; it never publishes partially and never treats emptiness as success.
- **Sizes.** Pages are split by schema and tenant as needed. Sums are recomputed by streaming at publication; there is no unbounded buffer.

### D4. Child repair, extended by ancestry
- **Recorded ancestry of J:** J = A0, A1, …, where (A_k, Some(A_{k+1})) is in N's `accepted`. It is finite and acyclic, since incarnations are never reused.
- **Rule:** a plan member (N, X) counts for requester (N, J) iff X is in that ancestry. It is checked at the requester (`plan_child`, Repair role) and at the source's requester test.
- **Sources** are only plan members present EXACTLY at their current incarnation, holding a Complete intent whose rows are verified (`serve_child_fetch`). A store that recovered a child through ancestry is not a source. When no source exists, recovery is blocked and reported; repeated replacement beyond that is not claimed solved.
- **Credit.** Copying never signs, acknowledges or counts toward any quorum.

### D5. Installation (F2)
- **Restricted storage.** `ControlOnlyNode::recovery_storage` runs ONE retained task that:
  1. stops the preparation cooperatively (never aborted, so an owner-bound open it began completes into its slot);
  2. has it hand its owner-bound index over: closed through the owner's own registered closer (`OwnedRedb::close_and_join`), whose single async gate makes take, check, drop launch and pending-join publication one transition, so concurrent or resumed closes share one drop;
  3. opens a restricted `SlabManager` under the same owner, with the root enrolled (ADR 0726). It builds nothing else.

  A cancelled caller never abandons a half-open store, and a failed open is final for the process. The preparation never reopens a handed-over index.
- **Rows** are stored by `ingest_recovered_rows` (the child row path, with no intent or owner) and by `append_tombstone`. They are durable before their page's entries.
- **Pages** go into `base_recovery_v1` on a pristine store only (`stage_recovered_page`), and each page is committed with its progress.
- **`publish_recovered`** activates at G in one transaction, only when the recomputed sums equal the certificate's chunks.
- The ordinary `stage_base` / `activate_base` fences are unchanged.

### D6. Proof and record
- **The proof**, computed from J's actual store at the cut:
  - the base is active at G with rows hashing to the certificate's chunks (or G = 0 with an empty base);
  - `entry_is_stored` passes for every entry;
  - every D2 child with dec ≤ K is Complete with verified owned rows.
- **The saved submission.** Before the first send, J durably saves `RecoverySubmission {T, r_new, B, J, G, target_digest | None, K}` and never re-samples it.
- **The record.** J signs `RecoveryProved` with that tuple. The reducer admits it only if D1 holds and G and the digest equal the current certificate (or G = 0 with none), once per token; an exact retry answers its historical success. It checks fields, never bytes. J signs only after its local validation.

### D7. Restart
1. Recheck D1.
2. Read the applied record for the token.
   - **Absent:** G is fixed (the store's own report is owed to every commit). Validate the saved tuple's data locally and submit it unchanged.
   - **Equal**, and the statement saved here is exactly it: the record is historical. At each start, before recovery counts as complete:
     - owed children through K are repaired if missing;
     - the stored base is validated again from its actual entries (sums recomputed, every entry stored);
     - every owed child is Complete with its rows verified.

     A damaged base fails closed (no reset). Without certification participation the current certificate must still be the recorded one. If it moved, recovery fails closed as an unsupported transition (the ordinary report → commit → activate path is ADR 0107).
   - **Different, or a record with no saved statement:** stop. This is a defect, never overwritten.

### D8. After the record (ControlOnly)
The recovery task keeps repairing the delta (children Accepted after K that name the slot), scanning every retained Accepted parent each pass, so none is skipped. "Delta current" means: every such child applied by the pass's read is held. It does not mean no later work exists. NotServing still answers data requests. No lease, coordination, serving, election, tier upload, reclaim or certification report runs (certification participation is ADR 0107).

### D9. Slot-keyed receipts
- **Index readiness** keeps ADR 0104's promote-only semantics. J rebuilds its indexes from its recovered rows, and completion is recorded per (slot, J). Gate 4 requires it.
- **Drop reclaim:** J attests for its own store only. Slot N's settlement for I's objects stays open while I's reclaim is deferred.
- **Logical backup** (ADR 0736) is expected to proceed through the ordinary query route while N is ControlOnly (NotServing NOT_LEADER, then a client redirect). This is a control, not yet a demonstrated pass.

### D10. Compatibility (pre-release, in place)
- PD WAL/snapshot 26→27, marker v16→v17 (v16 retired), frame 19→20: a v19 peer would refuse or misjudge `RecoveryProved` and the fetch.
- New `MsgType` TABLE_BASE_FETCH_REQ/RESP.
- New store tables `base_recovery_v1` and the meta keys `recovery` and `recovery_submission`.
- Today is unchanged.

## Control map
1. **Genesis:** no certificate; children only; G = 0 record.
2. **Nonzero predecessor:** install, publish, reopen, `entry_is_stored`. A crash before and after publication; a sums mismatch under the same certificate is refused.
3. **Fetch refusals:** stale or foreign (T, r_new, B, J); wrong gid or digest; a page truncated, extra, reordered, with a wrong digest or position, or over bound. Each falls to the next source, and nothing publishes.
4. **Ancestry:** I → J1 → J2. J2 repairs a child naming I; the direct-link mutant fails; a successor-only holder is not a source; no ack.
5. **Delta:** a reservation at or before K that is Accepted after K is repaired. The cursor never skips an unrepaired child.
6. **Restart:**
   - a restart after the record (equal record and saved statement: also the lost-reply case): revalidated and continuing;
   - an already-recorded store reopened with a damaged base: stops, never "recovered";
   - a conflicting record: stop;
   - a later replacement: stop.

   (Record, then the next generation, then a restart is ADR 0107.)
7. **Barrier:** after `RecoveryProved`, a target commits only once the promoted store reports itself; the survivors' reports and the record do not stand in for it. (Report vs activation across a crash is ADR 0107.)
8. **Containment:** NotServing still answers; no lease, coordination or election; I's objects and pointers are untouched.
9. **Backup:** an export with N ControlOnly completes, and its digests match.
10. **Daemon (later, after review):** the replacement run extended. Rows written before the loss and after promotion are recovered; status shows the record; J stays ControlOnly.

## Consequences
- Certified-generation progress stays blocked from promotion until ADR 0107 (the promoted store must report); retained decision data is not erased by certification meanwhile.
- P1 (ADR 0105) waits for the cluster's first write: takeover needs the sealed placement. A never-written cluster cannot promote a replacement (F2 finding).
- Recovery can block if a needed child has no exact plan member left to serve it.
- J holds a full copy of the slot's data, but still serves nothing until gate 4.

## Not decided here
- ADR 0107: taking part in certification (the ordinary lifecycle on the restricted store, ancestry in `certify_pending`, report → commit → activate across restarts, the record-then-next-generation restart).
- Gate 4 (activation, the role change, election, serving, the lease/HLC bindings, the activation-cut proof, and the index rebuilds and receipts of D9).
- Reclaim of I's objects.
- Successor-source serving.
- Repeated replacement beyond D4.

## As built
- **Reducer.**
  - `DecisionCommand::RecoveryProved { proof: Signed<RecoveryBody> }`, where `RecoveryBody {token, slot, r_new, birth, store, gid, target_digest, k}` is evidence bound to (slot, store).
  - `record_recovery` checks, in order: the signature; the identity fields; an exact recorded statement, answered as its historical success (another is `RecoveryConflict`); `instance_serves` (shared with ADR 0105's promotion) and the current incarnation; the current certificate, or (0, None) at genesis (`RecoveryStale`); `k < log_index` (`RecoveryCutAhead`).
  - It records `Replacement.recovered: Option<Recovery {gid, target_digest, k}>`. `ForwardedDecision::RecoveryProved` is admitted only when `body.slot == sender`.
- **Authority and ancestry.** `decisions::current_replacement` is the D1 check used by the base fetch source. `decisions::in_recorded_ancestry` follows N's `accepted` links. It is used by `plan_child` (Repair role only) and by `fetch_source_check` (requester eligibility). The source itself is still required to be an exact plan member.
- **Repair.** `repair::RepairContext` plus `repair_child_in`; the data server's `repair_child` delegates to it with its factory connect. A Repair persist never signs (`child_writes.rs`).
- **Base fetch.** `network/data_server/base_fetch.rs`:
  - `TABLE_BASE_FETCH_REQ`/`RESP` (0x6E/0x6F), version-1 frames, routed to the data ring, and in the PD served set;
  - `BaseFetchRequest` carries the full replacement identity;
  - the source answers through `handle_base_fetch` / `serve_base_page`: D1, the current certificate, the base validated, not behind and active at gid; pages in key order of one (tenant, table), at most 1024 rows, rows projected with `stored_row_projected`;
  - **byte bound (Codex 01a1e20e R1):** the charge is what is sent, the answer meta plus the encoded Arrow payload; each row is charged by its own encoding. A page carries rows up to the request's `max_bytes` (normally `MAX_PAGE_BYTES`, 4 MiB). A first row that alone exceeds it is refused `RowTooLarge`, never sent; the receiver asks again once at `MAX_ROW_PAGE_BYTES` (= `MAX_CHILD_ROWS_BYTES`, 64 MiB, the bound any stored row already meets on the child path). The source refuses an answer above `max_bytes` plus a 1 MiB meta allowance, and the receiver checks the frame header against the same bound before reading the body. The total answer cap is therefore 5 MiB normally and 65 MiB enlarged (accepted, Codex 01a1e238); it is not a strict 4 MiB page and not a small-node memory result. Above the 64 MiB row payload a refusal blocks recovery, reported;
  - `with_rows`: phase 1 asks for entries only (a page with rows is refused), phase 2 for rows;
  - the receiver checks with `check_page`; rows pair with entries **by position** (R2): row i must be entry i's identity and content, so a permutation is refused before anything is stored.

  Pages walk the whole base by key cursor, not chunk by chunk. Completeness is the publication check: streamed per-chunk sums, counts included, must equal the certificate's. NotServing answers the fetch `Refused(NotReady)`.
- **Install.**
  - `stage_recovered_page` / `publish_recovered` (`base_recovery_v1`, meta key `recovery`; pristine store only; sums recomputed while reading);
  - `ingest_recovered_rows` (the child row path without intent or owner, grouped by HLC);
  - `deletes::append_tombstone` for tombstones.

  **Two phases (R4):** untrusted data never reaches the store's versions.
  1. From one source, entries only, staged in `base_recovery_v1` and checked whole against the certificate (counts and sums, streamed). A mismatch discards that staging (`discard_recovered_staging`, metadata only, pristine store only) and the next source is tried. The staging is bound to the certificate before any entry arrives, so an **empty base at a nonzero gid** (R3) verifies and publishes against the empty sums.
  2. Rows, page by page from any source (resumed at the cursor across sources): a page is ingested only when its entries are exactly the next staged entries after the cursor (a contiguous prefix, read from the staging in key order) and each row is its entry's; a last page (or an empty one) must reach the staging's end. A gap, skip or early end is refused before anything is stored, and the next source is asked from the same cursor (Codex 01a1e238 R1). Then the staged base is published.

  A source whose entries differ is never used for rows; nothing it sent is stored.
- **Saved statement.** `RecoverySubmission` (meta key `recovery_submission`), written once (`save_recovery_submission` refuses another).
- **Driver.** `node::replacement_recovery::RecoveryDriver`, run by the preparation task after promotion and binding:
  - it hands its owner-bound index over and opens the restricted `SlabManager` (it waits while `RootKekNotEnrolled`; any other open failure is final);
  - each step: D1; with no record, install the base at G if G > 0, repair owed children with dec ≤ K, `validate`, save the submission, sign, then `Progress::Ready`, which is forwarded (`RecoveryRecorded` is "again");
  - with a record: reconcile exactly, require the certificate unchanged (otherwise `RecoveryUnsupported`), revalidate once per start, then repair the delta;
  - `validate` reads the base in bounded pages (1024 entries) with 256 fixed chunk accumulators (`ChunkAccumulator`), recomputing the sums from the stored entries (never the stored metadata), and runs `entry_is_stored` (a shared free function now) on each entry and `verify_owned_rows` on every owed child;
  - the delta is selected by decision index (`owed_children`, a pure function): a parent reserved at or below K and accepted above it is owed after K;
  - test seam `Stage::Recover` (the storage opens, no step runs); `ControlOnlyNode::prepared()`.
- **Formats.** PD WAL/snapshot 27, marker v17 (v16 retired), PD Raft frame 20, golden `parent_decisions_v27.txt`: `promoted_replacement` moved (it gained `recovered`), and `recovery_proved` added.
- **F2 lifecycle (Codex 01a1e181, 01a1e1b2):** the owner-bound per-store close through the owner's registered closer, under one async gate, with a failed drop-thread launch restoring the database; a cooperative preparation stop; F2's two parallel-only hangs recorded (F2-CORRECTION-55).
- **Owning-run oracle (R5, Codex 01a1e238 R2):** test-support `recovered_rows_for_test` (in-process `recovered_rows_in`) opens a stopped replacement's store as its recovery opens it, under its own directory owner, and returns the saved statement, each Complete child failing `verify_owned_rows` (owner, physical copy, content digest), each base row entry with no or a differing stored copy, and every stored row version read from the slab and projected. The workflow requires the exact statement, no failure, and the rows read equal to the acknowledged ones, one copy each; each read runs once within an absolute bound checked before it is accepted, and a timed-out read is never cancelled inside an open. The record promises its cut K, not every acknowledged write (a write accepted after K is repaired as delta afterwards; owning run pd-replace-20261116T030142Z FAILED on exactly that oracle overclaim, row 8 accepted at 178 > K 147). So each read follows one declared 30 s wait with that single process running: once after the record, and once after a restart with a row written then. Then one stop and one read within one absolute bound. No restart-until-held. The next owning run, pd-replace-20261116T031854Z, passed every gate-3 check but FAILED later on a test defect of mine: the post-restart row reused id 9, which the old-instance step writes again; that row is now id 11.
