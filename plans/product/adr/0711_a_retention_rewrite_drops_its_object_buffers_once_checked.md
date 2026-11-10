# ADR 0711: a retention rewrite drops its object buffers once checked

Status: proposed, 2026-11-10 (Claude, Codex). Direction: Codex 01a1c70d (the one reviewed §8 candidate); production delta accepted (Codex 01a1c735). Implementation lands with this plan. A reduction of transient buffer lifetime, not a §8 fix. Review record: `reviews/s8-reconciliation/` (S8-RECONCILIATION §9, CODEX-S8-STEP0.txt, CODEX-LIFETIME.txt).

## Why
- **The overlap (source, main 58899ada6).** The Today retention rewrite's `Rewrite` arm (`storage/sweeper.rs`, `rewrite_one_tiered_slab_for_retention`) owns two whole-object buffers:
  - `encrypted_bytes`, the new encrypted object;
  - `fetched`, the same object read back from L2, used only for its content hash.
  
  Both stayed in scope to the end of the arm. After the footer summary, the arm needs only the object's length. Yet both were held while the proposal committed and the observer delivered it. That delivery fetches and decodes the new object again and, over a locally published source with a different payload hash, the source object (ADR 0705 §4–5).
- **What this is not.** It is a reduction of transient buffer lifetime on one path. It is not a §8 fix. §8 (ADR 0698) stays FAILED. Any RSS benefit is unproven until the frozen acceptance campaign measures it.

## Decision
**The arm drops the read-back as soon as its content hash is computed, and drops the encrypted object right after its footer summary, keeping only its length. Nothing either buffer is checked for changes, and no prepared bytes cross the authority boundary.**

1. **The read-back.** `fetched` lives in a block that yields its digest and drops it. The hash comparison, the cleanup of an unsubmitted candidate and the error are unchanged.
2. **The encrypted object.** Its length is kept as `encrypted_len`, and the object is dropped after `encrypted_tiered_footer_summary`. The failed-commit cleanup receives `encrypted_len`, the same value as before.
3. **Unchanged:**
  - the upload-URI check and the ambiguous-upload intent;
  - the read-back hash;
  - the footer;
  - the candidate preparation and the proposal;
  - the apply;
  - the observer's delivery, with ADR 0705's classification and its acknowledged-record evidence;
  - the order of every await.

  No concurrency or allocator change.
4. **Out of scope:**
  - the preparation's own peak (source ciphertext and plaintext, rewritten plaintext and new ciphertext, `retention_reclaim.rs` `build_tiered_replacement`), which may set the operation's peak;
  - the delivery re-fetching objects the leader already holds, which would move prepared bytes across the boundary ADR 0705 has the delivery validate;
  - the encrypt and decrypt internals.

## Acceptance (finite)
1. **Source review of the lifetime itself.** Each buffer's last use is shown in source: `fetched` ends where its digest is computed; `encrypted_bytes` ends after the footer summary, and only its length is used afterwards. Neither is referenced across `prepare_tier_candidate_proposal`, `propose_tier_slab` or the apply.
2. **A manual capacity census, kept in the review record and not a permanent control.** In a one-off diagnostic (s8life-r1/r2, base 58899ada6), a hand-enumerated probe of the two selected buffers summed 1,586,320 bytes held across the proposal for a 793,160-byte fixture object before the change. The "after" zero follows from the source lifetimes, not from an independent allocation measurement. The probe listed its buffers by hand, so neither run is a regression control proving the drop. It is not part of this change.
3. **Existing controls pass unchanged:** the sweeper's retention-rewrite set, its ambiguous-commit control, the manager's retention-rewrite set, `architecture_test` and `three_node_workflow`. In s8life-r1, 227/227 passed (206 architecture, 11 sweeper, 8 manager, 2 three-node workflow).
   - The hash-mismatch controls included (`b2_hash_mismatch`, `b1_untyped_would_block`) exercise other upload paths, not this Rewrite arm.
   - This arm's own hash-mismatch branch is not exercised. The change only scopes `fetched` before the same comparison, so that branch is unchanged.
4. One checkpoint at the tip. No load campaign.

## Not established
- Any effect on RSS, on the operation's peak, or on §8. The prepare peak may dominate.
- Behaviour in ParentDecisions mode and on three nodes; this is the Today path only.
- DC-201 and the other registered limits.
