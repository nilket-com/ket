# ADR 0728: an encrypted tier pointer is accepted only while its tenant key is present

Status: accepted, 2026-11-13 (Claude, Codex). Defect DC-231, source-supported since ADR 0726's design review (Codex 01a1d3b5), reproduced and its correction agreed by Codex 01a1d597 (`reviews/dc231-20261113/`).

## Why
- An upload encrypts a sealed slab under the tenant's active key version, uploads the object, and then asks the control ring to make the object the slab's pointer. A key version may be retired once it is not active and no committed pointer names it. An upload still on its way to the ring is not a committed pointer, so its key could be retired underneath it.
- The ring then accepted the pointer anyway: the tier operation's validation checked provenance, terminal state and the generation it replaces, and never looked at the tenant keysets.
- Reproduced on main 03d051da7, one execution each, debug build (`reviews/dc231-20261113/red-r1.diff`, sha256 43507a46…; bigfoot `~/work/reviews-logs/dc231-dev/red1.log` d0e6fbce…, `red2.log` 6a5b75df…):
  - At the reducer: with a retained version A and an active version B, retiring A succeeds, and a first publication naming A is then decided Applied.
  - At a real single node (Today mode, local-directory object store, development provider): a first publication held inside the production committer's `submit_op`, after every local check and before the ring sees it, while A was rotated away and retired through the ring. Released, the pointer committed naming A, the node's tier map took the slab, and a read of the row failed: "tenant 0 does not have tenant_kek_id …".
- What that run does not show: the row was appended to storage directly, not acknowledged to a client through the data server; the map transition was observed, not the file's absence; it does not prove no copy of the retired key is recoverable anywhere. It is an unsafe committed pointer and a failed normal read, on one path, once.

## Decision
1. **One check in the shared tier validator.** After its existing checks and before any effect, a `Tier` operation whose pointer is encrypted must name a tenant and a tenant key version, and that version must be in that tenant's keyset in the state the operation is decided in. Otherwise the operation is rejected. A plaintext pointer names no key and is unaffected.
2. **Presence, not activeness.** A version that was rotated away and not retired still opens its objects, and a pointer naming it is accepted as before.
3. **Every tier operation passes through it**: a first publication, a replacement, a metadata refresh and a certified publication, in both modes, since `apply_valid_command`'s insert is the only place the product writes a pointer into state. A successor sealed under its source's key and a refresh pass because the pointer being replaced holds that version against retirement until the same apply.
4. **Two rejections, appended**: `TenantKekAbsent { tenant_id, tenant_kek_id }` and `EncryptedKeyReferenceMissing`. A rejection is recorded like any other: no pointer, no observer event.
5. **A recorded operation is not judged again.** `admit_op` answers a duplicate with its recorded outcome before validation, as it did: a rejected publication stays rejected if the version reappears, and an applied one keeps its answer after its pointer and key have legitimately gone.
6. **Nothing changes at the proposer.** A rejected publication already hands its candidate off durably as a rejected candidate, resolves its reservation and returns an error; the local slab file is untouched and the next pass seals it again under the then-active version.
7. **Fresh formats, in place.** Today WAL and snapshot 19 to 21, ParentDecisions 20 to 22, the mode marker to `parent-decisions-v12`. Today's Raft frame version 13 to 15 and the ParentDecisions envelope 12 to 14. The general HRFT version stays 1. Earlier stores and peers are refused, not converted.
   - The WAL holds commands, and its record bodies are byte-identical; it is bumped because their replay changed. An earlier log may hold a publication that applied and would now be rejected.
   - Snapshots and forwarded receipts can carry the new rejections.
   - A peer of the earlier rule and a peer of this one must not replicate one tier entry and decide it differently, so the frame versions move and the previous ones are refused before decode, forwarded ParentDecisions operation answers included.
   - The backup manifest's own version is unchanged; its inner WAL and snapshot versions are validated as before.

## Algebra
- The key of the rule is (tenant, tenant key version) as named by the pointer being decided, looked up in that tenant's keyset in the same state.
- Retirement of a version and acceptance of a pointer naming it are both decided by the one reducer, so they are ordered. Pointer first: retirement refuses on the existing count of committed pointers. Retirement first: the pointer's operation is rejected.
- **The invariant is about decisions, not about every reachable state:** every encrypted tier pointer accepted from this format on named a version present at its decision, and ordinary version retirement cannot remove a version a committed pointer names. It is not "every encrypted pointer always has its key": in Today mode a tenant that reaches Deleted has its keyset removed while its pointers remain (`apply_delete_tenant`); ParentDecisions refuses tenant deletion. That lifecycle is unchanged here.

## What this does not do
- It does not delete the rejected candidate object. It stays uploaded and unreferenced, recorded as a rejected candidate, as every rejected publication's object already does.
- It does not change tenant deletion, root rotation, key escrow, local key caches, or the proposer's local `still_current` narrowing.
- It adds no reservation or lease of a key version for an upload in flight: an upload that loses the race is rejected and repeated.
- It does not convert or repair a store that already holds such a pointer; earlier formats are refused.
- DC-222, DC-223, DC-224 and the failed memory acceptance are unchanged.

## Acceptance
1. Both reproductions pass with the fix: the reducer test, and the real-node test, which additionally shows the local file still present and the row readable after the rejection, then a later pass publishing under the active version and the row readable through the pointer.
2. The controls pass: publication before retirement makes retirement refuse; a retained inactive version is accepted; missing identifiers and an absent tenant or version are rejected; replacement and refresh; replay of a recorded rejection and of a recorded application; a follower's owner-signed publication in ParentDecisions mode.
3. Format controls: the goldens under their new names, refusal of the previous stored formats, markers and frame versions.
4. The focused selection passes, the architecture tests and the 2 MiB worker regression included.
5. The real three-daemon test passes once on the product tree to be delivered.

## Validation record
Development evidence on bigfoot, `~/work/reviews-logs/dc231-dev/`, debug builds, one execution each, working tree before the commit.
- red1, red2: the two reproductions on exact main, described under Why. Both FAILED as reproductions should.
- g1: did not compile (an error in the new node test).
- g2: 315 run, **4 FAILED**: two "newer build" tests whose unknown version, 21, is now Today's (moved to 23); the new node test's assertion that the rejected candidate object is deleted, which was wrong about existing behaviour (corrected to assert its record and its presence); and `a2_reconcile_never_rolls_back_same_layout_tenant_kek_rewrap`.
- f1 (sha256 5e99de96…): the focused selection, 928 run, 922 passed, **6 FAILED**: five follower-offload tests and the architecture count of state-machine writers. Cause of the five, and of a2: those fixtures' state machines held no tenant keysets while their storage manager encrypted under keys of its own, so the new rule rejected their pointers. The fixtures now seed the state machine with the same keysets, through a new test-only helper, which is the one the count now includes. That makes the fixtures consistent; it is not evidence about how a production node's keys are delivered.
- f2 (a423e240…): those tests and the tenant key tests, 38 of 38 passed. f3: the two previous-format refusal tests passed. `cargo fmt --check` clean.
- **f4 (7f07bdfa…): the focused selection on the final bytes, 930 run, 928 passed, 2 FAILED. Acceptance item 4 is unmet.**
  - `certified_tier_cleanup_rewrites_and_retires_the_leaders_tiered_slabs`: one retained object where two were expected. DC-222's signature, the same as in ADR 0717's checkpoint and ADR 0727's focused run. It passed in f1.
  - `three_nodes_keep_every_acknowledged_row_through_tiering_restart_and_failover`: after "leader 3 stopped, 2 took over", row 11 was never acknowledged within the workflow's 390 s; last error "simulated transport target 3 is not published". DC-224's signature, seen in ADR 0718's and ADR 0725's checkpoints. It passed in f1, in 16 s.
  - Neither was rerun. Both tests publish encrypted tier pointers and one changes frame versions' peers, so neither is claimed independent of this change; both failed this way before it existed. Not classified as flakes.
  - In f4 `table_writes_are_served_within_a_daemons_worker_stack` passed, as did both architecture binaries and every new test.
- Selection: the library's consensus, sweeper, node, wire, control server, operation index and tenant key tests, and the binaries architecture_test, mutation_visibility_architecture_test, three_node_workflow, upgrade_compatibility, backup_restore, multinode_bootstrap and control_ring_rpc.
- **The real three-daemon test PASSED, once,** on impl ab32a36cb8c705089da1a9f21355bd3271d334ae (tree 2452dbb16931169e9b9d0413fe4f01dd1c056e00), unchanged launcher, debug build: bigfoot `~/work/reviews-logs/pd-smoke-20261113T162057Z`, seal 290a3ce9bb66ad8b10e63be6916f55fc50f61a6c9ea4f51abbf80c71aa5cbafa. It forms a ParentDecisions cluster under the new formats and envelopes, serves a client and restarts a follower; it does not exercise the rejection. The delivered tree differs from that one only in this record's text.
- Not run: a shared full checkpoint (agreed, Codex 01a1d597); a release build; three hosts; a ParentDecisions cluster of real nodes losing this race (the mode is covered at the reducer only); a client-acknowledged write as the row.
