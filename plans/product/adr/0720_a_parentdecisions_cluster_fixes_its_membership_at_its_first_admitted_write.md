# ADR 0720: a ParentDecisions cluster fixes its membership at its first admitted write

Status: proposed, 2026-11-12 (Claude, Codex). Census, trace and design reviewed by Codex 01a1d118, 01a1d11d, 01a1d124 (`reviews/pd-daemon-20261112/`). Implementation lands with this plan. This is the boundary Codex required before a daemon may select ParentDecisions; daemon selection is a later ADR.

## Why
- ParentDecisions moves no data. Migration seeding is disabled under it, with the comment "The fixed membership forms before any write is served" (`network/control_server.rs`). **Nothing enforced that.**
- A join is three separately committed things: a learner entry, a voter change (joint, then uniform) and an epoch advance. A write could be admitted between any two. Nothing compared the voters with the epoch's peers, and nothing durable recorded that writes had begun: parent records, retry keys and noops are all pruned.
- What a later member change did: lineages pruned, in-flight parents refused, the new member refusing every read that touches earlier data, and generation commits stalled for good. Reads failed closed, but the cluster was degraded permanently and silently.

## Decision
Definitions. The **placement** of an epoch configuration is the set of its peers, each with whether it holds the Data role; a ring is built from nothing else (addresses, tiers, capabilities and the epoch number place nothing). A store is **formed** when its applied Raft membership is uniform (not joint), holds no learner, and has exactly the latest epoch's peers as its voters.

1. **The seal.** The decision state gains `sealed`, the placement fixed by the first admitted Reserve. It is set by that Reserve's apply, after every existing check has passed, and only if the store is formed. It is never cleared and never pruned, and it is carried in snapshots.
2. **Before the seal.** A Reserve that would be admitted while the store is not formed is refused `FormationIncomplete`: no record, no seal. A Reserve refused for any other reason does not seal.
3. **After the seal.** A new Reserve is admitted only while the store is formed with the sealed placement; otherwise it is refused `MembershipSealed`, state unchanged. **A parent already on record still answers as recorded**: the existing lookup comes first, so the fence never hides an outcome from its retry.
4. **Epoch advances.** Once sealed, an advance is refused whole (no epoch recorded, no lineage pruned) if any configuration it would introduce has a different placement. Both ends are judged, because both are recorded and the highest epoch serves. An advance with the same placement applies as before: an address, tier or capability change, or a same-members renewal. Lineages are then regranted at the new epoch by the existing mechanism and writes resume. **Placement is fixed; the epoch number is not.**
5. **Join requests** to a sealed cluster are answered from applied state, after the existing linearizable check and under the existing reconcile lock, and start no membership entry:
   - a member whose address, tiers, roles and capabilities are the ones on record, in a formed store whose latest epoch is the published one, gets success (the repeat of a completed join);
   - a non-member, a member with any different property, or any request while that view is incomplete, is refused `MembershipSealed`.
   A changed property is refused **through a join**; rule 4 still lets a same-placement epoch advance change an address or capability.
6. **A refused advance is an error to its caller.** The proposer used to ignore the state machine's answer; it now returns the refusal.

## What is prevented and what is contained
- **Prevented, in log order and across leader changes: a change of placement after the first admitted write.** The ring exists only as epoch history, and rule 4 is in the reducer.
- **Not prevented: an openraft membership entry** already queued, or proposed by a leader that has not applied the seal. The reducer cannot refuse those; rule 5 only keeps well-behaved joins from creating them. After such an entry applies, the voters differ from the sealed members while placement is unchanged. That state is **contained, not repaired**:

| Path | What it reads | In that state |
|---|---|---|
| New Reserve | rule 3 | refused `MembershipSealed` |
| A recorded parent's retry | the record | answers as recorded |
| Finalize, Resolve, Abort, RecordNoop | lineage at the latest epoch, the stored plan, incarnations | unchanged: parents reserved earlier finish or abort |
| Child holder, replica and fetch work | the stored plan, the lease and lineage at the latest epoch | unchanged |
| Reads | the reader's incarnation, the latest epoch's peers, stored plans | unchanged for sealed members; a stray voter is not an epoch peer and refuses |
| Lease grants, signer binding, incarnation registration | the epoch ring and the incarnation registry | unchanged |
| Generation targets, reports, certificates, cleanup | the epoch's peers at their current incarnations | unchanged |
| The leader's clock gate | applied voters | refuses to stamp during a joint configuration; availability only |
| The stray node's epoch advance | rule 4 | refused |

  Nothing that places or reads data consults the voter set, so existing data stays served from the sealed ring. New writes stop because the cluster's Raft membership is no longer the one it sealed. **Recovering from that state is outside this checkpoint.** Writes are admitted again if the applied membership returns to the sealed one, but no supported operation does that.

## Algebra
- Identity of the fixed thing: the placement, a set of (node, holds Data). Key stored: exactly that set. The voter set is not stored separately: at sealing it equals the placement's nodes, and "formed" re-derives the comparison at every admission.
- Reserve admission after sealing: `formed(state) ∧ placement(latest epoch) = sealed`. Since formed implies voters = latest peers, a voter set equal in ids but with a changed Data role is a different placement and refuses.
- Epoch advance after sealing: for every configuration `c` the advance would insert, `placement(c) = sealed`. By induction every epoch recorded after the seal has the sealed placement, so the serving ring's placement never changes.
- Not narrower than its identity: the seal is per store's decision state, replicated; there is no per-tenant or per-table membership.

## Formation
- A Reserve applied between the uniform voter change and the epoch advance, or while a learner exists, or during a joint configuration, is refused `FormationIncomplete`.
- **A write admitted before formation completes fixes the cluster at what had formed**, including a single node. A join that follows is then contained as above. The procedure is: form the cluster, check its membership, then write. No expected-size setting is added; cluster size stays derived from membership.

## Compatibility
- ParentDecisions control-log and snapshot format 16 → 17, store marker v8 → v9, Raft envelope 8 → 9. Today's formats (15) do not change: the decision state is carried only in ParentDecisions snapshots, and no command body changed.
- **A format-16 store is refused whole by this binary and is never read as unsealed**: it may have admitted writes it no longer holds a record of. It stays readable by the binary that wrote it. No existing store is modified, reset or deleted.
- Snapshot validation refuses a decision state that holds parent records and no seal, or an empty seal.

## Acceptance
- Reducer: the first admitted Reserve seals and a refused one does not; an unformed store (joint, learner, voter not in the epoch, epoch peer not a voter, no applied membership) refuses without sealing and admits once formed; a sealed store refuses a placement-changing advance, including an incompatible `from` at the higher epoch, with epoch history and decision state unchanged; a same-placement advance applies and a Reserve is admitted after regrant; after a voter change a new Reserve is refused while a recorded parent's retry, a Finalize and an Abort of earlier parents succeed, and admission resumes when the sealed membership is back; the seal survives encode and decode and the retirement of every record, and validation refuses records without a seal.
- Join: the completed-member check accepts only an identical member in a complete view and refuses a non-member, each changed property, joint, learner, stray voter, unapplied membership and an unpublished epoch.
- Three nodes: after the first write a fourth node's join adds no voter and no learner on any member, the seal names the three, and a later write is admitted.
- Formats: the format-17 corpus differs from format 16 only by the seal appended to the decision state; format 16 and marker v8 join the retired, refused set.
- One checkpoint gate on the final pair.

## Limits
- A sealed cluster cannot grow, shrink or replace a member. That was already true in effect (no data movement, no replacement, no backup); it is now explicit and enforced for placement.
- The three-node join test observes membership through the joiner's refusal or an eight-second bound; it does not drive a repeated join by an existing member through the handler (that path is covered by the check's unit tests only).
- A forced post-seal voter change is tested at the reducer, not by injecting a membership entry into a live cluster.
- Not addressed: daemon selection and decision keys (next ADR, including the forwarded-request version boundary Codex required), recovery from a contained state, backup, replacement, DC-223, DC-224, section 8 (FAILED, unwaived).
