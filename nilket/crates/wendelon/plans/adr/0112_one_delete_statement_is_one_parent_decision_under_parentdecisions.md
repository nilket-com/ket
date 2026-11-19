# ADR 0112 - One delete statement is one parent decision under ParentDecisions

## Status
Proposed, 2026-11-19. The design was ruled by Codex 01a1f295 (after DELETE-01 and DELETE-02, and reviews 01a1f291 and 01a1f28a). Implementation follows its failing controls. This is the second unit of the cutover to ParentDecisions as the sole serving decision authority (product ADR 0738 was the first). Its acceptance checks found two defects already on main, each delivered first as its own fix: recovery refused a partitioned table's row tombstone, so any SQL DELETE left a local profile unable to start (`44069b942`); and a refused start consumed the store's clean-shutdown record (`1c0211265`).

## Context
- **The failure:** a multi-row SQL `DELETE` through the public ParentDecisions path (`nilket local init --decision-authority parent-decisions`) fails whole: `table write rejected: PBC_REJECT_ROW_COUNT_MISMATCH`. No row changes. Single-row deletes work.
- **Toron already does its part:** it sends every candidate row id in one `TABLE_DELETE` request, under one caller-minted operation id that every retry reuses (ADR 542). An empty match is answered without a request.
- **The one-row limit is in Wendelon only:**
  - the coordinator: `deletes.rs`, `let [row] = row_key_ids`;
  - the decision types `DeleteRequest`, `DeleteSelection`, `NoopBody` and the request digest;
  - the reducer's matched-delete checks (one child, one row);
  - the child's payload check.

## Decision

### D1. One request, one identity, one selection, one parent
- **The request:**
  - **Shape:** a request names 1 to 512 row ids, **canonicalized at ingress**: sorted by identity bytes; duplicates refused.
  - **Identity:** the caller's `ParentKey`.
  - **Digest:** over the canonical rows. Order of arrival does not matter; the set does.
- **The selection:** at one cut K, every row is proved by the evaluating node: a live winner (from accepted children or the certified base), a tombstone, or absence over the complete inventory. It is frozen as `DeleteSelection { cut, outcomes }`, aligned with the rows. **An unproved row stalls the whole statement; inability to prove absence is never "not found".**
- **No matched row:** one `RecordNoop` carrying the whole selection.
- **Matched rows:** one `Reserve` of one claim:
  - **its children:** the matched rows, grouped (D3);
  - **its coordinator:** the child holding the lowest matched position;
  - **its `commit_hlc`:** above every selected winner and every child lineage's floor;
  - **then:** the existing child writes and `Finalize`.
- **One parent, one terminal decision:** `Accepted`, `Refused` or `Noop`.
- **The receipt:** its counts are the frozen selection's (deleted, already deleted, not found). There is no per-row commit and no split.
- **Visibility:** every tombstone is a child version of that one parent. At any reader's cut, on any node, all of the statement's matched rows are visible or none are. A refused parent changes none.
- **What "atomic" covers, and what a receipt does not say** (ADR 0699, line 227): the counts describe this parent's selection at **its** read cut, and finalization publishes **its** tombstones together. They do not say that no other statement touched the same rows.
  - **Example:** two concurrent statements both select live row r at their own cuts. Both commit, and both report r as deleted. Each publishes its own tombstone, and a reader sees r gone from the lower `dec` on.
  - **Not offered:** no stronger isolation contract.
- **N = 1 goes through this same implementation**, with the same counts, statuses and retry semantics.

### D2. Routing: one eligibility predicate at every entry
**Lookup first.** A recorded decision answers from any node, with its counts and `dec`:
- `Accepted`/`Noop`: those;
- `Refused`: known-not-committed;
- another digest under the same identity: `WRITE_KEY_CONFLICT`;
- `Reserved`: only its recorded coordinator drives it.

**Recorded decisions replay before any fresh coverage is required.**

**Then, for an unrecorded identity,** from one coherent placement view (the current epoch, lineages and leases; rechecked when they change):
1. **The fallback holder F** is the holder of the lowest requested position.
2. **F does not cover the request:** **unsupported** (typed, no proposal) at every entry, before any evaluation.
3. **This node does not cover the request:** it redirects to F, which does.
4. **This node covers the request:** it evaluates at one K and computes the signer S (the holder of the lowest matched position, or F if nothing matched).
5. **After selection:**
   - **S is this node:** it proposes;
   - **S covers the request:** it redirects to S (`NOT_LEADER` with hint S), proposing nothing;
   - **otherwise:** unsupported (typed, no proposal).
6. **At S, the same rule runs again** at a fresh cut. There is no unconditional hop back to F.

A node **covers** the request when it is in the replica plan of every requested position. Topology never substitutes for proof at K: a covering node that cannot prove a row stalls.

**Hops:** in a stable cluster, a statement entering at a covering node proposes after at most one redirect; entering at an uncovered node, after at most two (to F, then to S). A concurrent change can move S between hops. Termination is then the caller's deadline and the client's attempt bound, never a silent loop.

### D3. Children and limits
- **Grouping:**
  - a child is a run of matched positions that is **contiguous in placement order**, inside **one lineage**, with an **identical replica plan** throughout;
  - a plan sequence A/B/A stays three children;
  - there is no grouping across lineages.
- **Order:** children are ordered by position. Inside a child, rows are in identity order. Positions are hashes and identities are keys; the two orders are never mixed.
- **Limits:**
  - at most 512 requested rows;
  - at most **64 children**;
  - plans of at most 16 members;
  - the claim and the noop at most 64 KiB encoded.
- **The practical limit is the child count.** A statement of 65 matched rows in 65 distinct lineages fails; hundreds of rows concentrated in few lineages fit.
- **A shape refusal:** a statement whose matched rows need more than 64 children, or a request over 512 rows, is refused **whole, typed, before any proposal**. A shape refusal does not bind the operation identity: it depends on placement and on the live set, and the same request can be admitted later.
- **Both sides check:** the coordinator before proposing, and the reducer on every claim and noop: canonical rows, aligned outcomes, the children partitioning exactly the matched rows, child shape, all bounds.
- **Bounded before materializing:** the rows and the outcomes are bounded at 512 when decoded (a longer sequence is refused before it is allocated), and the public ingress refuses a ParentDecisions statement whose declared count is over 512 (typed, batch too large) before its Arrow payload is decoded. Today keeps its own limits.
- **A lost answer while `Reserved`:** the coordinator re-derives each child's payload from the recorded claim and never re-evaluates.

### D4. Partial replication
Each row is proved locally. Under partial replication:
- **Unsupported (typed, no proposal):** a statement no covering fallback holder or signer can serve.
- **Served:** a statement whose rows a covering node and its signer replicate.

Remote point proofs at K, needed before ParentDecisions serves every shape at rf below the members, are later work.

### D5. Formats: a clean pre-release bump, ParentDecisions only
**The new versions:**
- marker `parent-decisions-v19`;
- WAL and snapshot format 29;
- decision and Raft envelope 22.

**Unchanged:** Today's versions and behaviour.

**A v18 store** (including one created by product ADR 0738) and its pending identities:
- stay usable with their matching binary;
- **the new binary refuses them at open, before any write, leaving every byte.**

**Not done:**
- no conversion;
- no lazy marker transition (a follower can receive a snapshot holding multi-row decisions without appending their commands, and snapshots serialize the decision state concretely);
- no reset;
- no cross-store receipt continuity.

A fresh v19 profile is a different store.

### D6. SQL
- **No Toron protocol change.** A delete refusal is reported as a delete, not as a table write: the client's `TableDeleteRejected` and Toron's `StorageError::TableDeleteRejected` ("table delete rejected: …").
- **A client names its rows** by `wendelon::schema::table_row_identities`, admission's own derivation.
- **RETURNING's SQL result stays distinct from the committed storage receipt.** A committed mutation whose RETURNING cannot be produced exactly is an SQL error over a committed mutation, as today.
- **The statement's predicate** is Toron's candidate scan. The selection at K deletes exactly the candidates live at K, and counts the rest.

## Rationale
- One parent gives one terminal decision and atomic visibility by the existing rules (ADR 0699).
- Canonical input makes the identity's digest independent of arrival order.
- The eligibility predicate closes the partial-replication cycle (an uncovered signer can never receive a redirect) without substituting topology for proof.
- The clean bump follows the project's pre-release rule. A lazy transition would need a storage upgrade protocol for snapshots and followers that nothing else requires.

## Consequences (The "Negatives")
- **The practical limit is 64 children.** On a three-node ring of about 192 lineages, a spread-out delete of a few dozen to a hundred rows may be refused whole.
- **Statements that a partially replicated tenant cannot serve** are refused, not proved remotely.
- **Existing v18 ParentDecisions stores need their old binary.** The new binary refuses them, and their receipts do not carry over.
- **The predicate is evaluated at Toron's scan, not at K.** A row inserted between the two is not deleted, as today.

## Acceptance
**Failing controls first:**
- **Wendelon, three voters:**
  - a delete spanning holders;
  - the paused-finalize visibility oracle: all rows of the delete visible before `Finalize`, none after;
  - an aborted parent;
  - the absent-at-F / live-at-B routing;
  - replay at a third node;
  - reversed order under the same identity, and a changed set (conflict);
  - a lost answer while `Reserved`;
  - 513 rows, duplicates, and more than 64 children;
  - an unprovable row (stall), a certified-base row, a missing row;
  - **RF2 partial replication** (A covers both, B covers only the live position), entering at A and at B, plus the partially covered case that succeeds.
- **The reducer:** the maximal claim and noop encodings; every malformed shape.
- **Formats:** marker v19 written; v18 refused by both authorities with bytes kept; envelope 21 refused.
- **SQL:** the public multi-row DELETE (the DELETE-01 live test, across a restart), a multi-row predicate, and an exact `RETURNING`. A `RETURNING` whose receipt differs from its rows is Toron's existing SQL error over a committed mutation (`toron/tests/delete_test.rs`, "DELETE RETURNING receipt mismatch"); no ParentDecisions daemon produces that race on demand.
- **Every existing single-row test.**

**Cross-binary, on disposable fixtures:**
- a v18 profile made by `c0358fba0` is refused byte-preserving by the new binary;
- the same profile reopens with `c0358fba0`.

**At delivery:** the real three-daemon process test, which gains one multi-row delete statement served on the daemons' own worker stacks, and the daemon-stack lane.
