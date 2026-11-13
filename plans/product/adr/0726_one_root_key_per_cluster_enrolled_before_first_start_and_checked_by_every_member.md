# ADR 0726: one root key per cluster, enrolled before first start and checked by every member

Status: accepted, 2026-11-13 (Claude, Codex); directory classification amended by Codex 01a1d400; source cleared by Codex 01a1d45b. **Its shared checkpoint FAILED 4,608 of 4,610 on the product code (two test fixtures, since corrected); no checkpoint ran on the final tree. See the validation record.** Operator 2026-11-13: "work with Codex on the DC-230 custody assessment". Assessment accepted and option agreed under the standing Claude+Codex agreement by Codex 01a1d3ac; design accepted by Codex 01a1d3b9 after holds 01a1d3b1 and 01a1d3b5 (`reviews/dc230-custody-20261113/`). 

## Why
- A tenant KEK is wrapped under the root KEK of the node that created it, and the wrapped bytes are replicated. Every root KEK provider generates its own random root the first time it opens a data directory, so a member other than the creator cannot unwrap the tenant KEK. Since ADR 0725 every member must (DC-230; found by that ADR's first focused run).
- The unlock secret (local-sealed) and the KMS key (aws-kms) protect the root at rest. They are not the root: equal secrets or equal KMS key ids do not give equal roots.
- Nothing identifies a root. A member with a different one learns it only when an unwrap fails, and reports it as a sweeper warning. Provider health says nothing about compatibility.
- A node whose root artifact has disappeared silently generates a new root on its next start, in every provider.
- The written model already assumes one shared root (`multi_tenancy.md`, ADR 020, ADR 031: "generate or enroll Root KEK material") without saying how a member gets it.

## Decision
Both authority modes. The three implemented providers (dev-insecure, local-sealed, aws-kms); pkcs11 stays an unsupported seam.

1. **One root per cluster.** The forming member generates it. Every other member is given the provider's sealed at-rest artifact, and the capability that opens it, before its first start. No unwrapped key crosses a network or touches a disk.
2. **A write-once root check in replicated state.** `RootCheck { version = 1, wrapped: 40 bytes }` is the existing AES-KWP wrap of one fixed public 32-byte constant under the root. Its id is the first 16 bytes of SHA-256 over the version and the wrapped bytes. The version is validated before anything else. It is a compatibility check value: not cluster identity, not proof of custody, not proof about any submitted tenant KEK.
3. **`EstablishRootCheck` transition.** Stored when state has no check, no tenant and no keyset. An identical value is a no-op (retries may leave duplicate log entries). A different value is refused `RootCheckConflict`. With no check but a tenant or keyset present it is refused `RootCheckAfterKeys`: a check is never laid over keys it did not witness.
4. **No tenant KEK before its check.** `CreateTenant` and `InstallTenantKekVersion` carry the id of the check their proposer verified against. The reducer refuses them with no check (`RootCheckMissing`) or another id (`RootCheckMismatch`). It compares ids only: no provider call in apply. This enforces ordering and the id relation under the existing trusted-proposer model; it does not prove the bytes unwrap.
5. **One establishment routine, two callers.** `establish_root_check_if_forming` is the only constructor of the transition. It runs at initial formation (fresh bootstrap, before the default tenant is seeded) and as the first step of every iteration of the default-tenant task, which is what runs after a restart. It acts only when applied state, read after a linearizable barrier on the local leader, shows: no check, no tenant, no keyset, exactly one voter (this node), no learner, and no membership epoch that ever held another member (a normal single-member start already records more than one epoch). Configuration grants nothing. A joiner or a recovered multi-member cluster never establishes.
6. **Joins wait for the check.** The leader's join handler refuses a join, retryably, while applied state has no check. The check is established only in a one-voter cluster and never removed, so no join can precede it.
7. **Only genuine formation generates.** A fresh store in bootstrap mode with no artifact generates a root. A fresh store with an artifact that opens adopts it (an operator-supplied root, or a formation interrupted after the root was persisted). Every other open is enroll-only: a missing artifact refuses startup and is never regenerated; a corrupt artifact, a wrong unlock secret or a denied KMS call refuses with the provider's own error. The artifact is read by the provider only: it grants no store freshness, identity, restore or join authority.
7a. **A root KEK directory is neutral to the setup censuses** (Codex 01a1d400; a deliberate, limited change to directory classification). Four predicates decide what a data directory is before a store exists: whether the previous shutdown was clean or the directory is new (`directory_is_fresh`), whether this boot may create storage (`fresh_storage_initialization_allowed`), whether a first creation was interrupted and may resume (`interrupted_first_creation`), and whether a directory may enter ParentDecisions (`admit_store`). Each refused any entry it did not list, so the enrollment procedure itself, and a forming node stopped between writing its root and creating its storage index, were refused. Now one shared classifier makes `.wendelon` neutral to all four, exactly as `LOCK` is, when it is a real directory that is empty or holds only regular files with the implemented providers' own artifact names. A symlinked directory or file, a nested directory, or any other entry is not neutral, and an unreadable directory is an error. Every other entry keeps its caller's existing rule; an interrupted creation still needs pristine control and no index in any form.
   - Neutral is not evidence. The directory authorizes no store identity, allocator position, incarnation, keyset or membership, and says nothing about the artifact's content: the provider still opens it and the committed check still verifies it.
   - **What this removes.** A store that had lost everything except its root file was refused, by accident of these lists. It is now treated as an otherwise empty directory is: a new store, subject to the existing incarnation, formation, sealed-membership and restore rules. A remaining root file cannot show whether the directory was pre-provisioned or once used. This is no claim that an old store was recovered or its data kept, and no permission to wipe one.
7b. **Pending is retryable, mismatch is not.** A refusal for a root not yet verified has the error kind the storage layer already retries on (as "tenant key not found" was before); a mismatch has a kind it never retries.
8. **The verified binding owns the keys.** The crypto manager holds one cell: `Unverified` (a loaded provider, no keys), `Verified` (a binding: check id, provider, the tenant keysets, and both key caches), or `Mismatch`. Every key function, every cache read and every cache insert is a method of the binding. Each public crypto operation begins by taking the current binding or failing `RootNotVerified` / `RootMismatch`. Replacing the binding makes the old one unreachable to every new operation, including its caches.
9. **Admitted work may finish.** An operation that took a binding completes under it; creating a read plan is the admission point. An upload asks again (`still_current`) immediately before it submits its tier pointer and abandons the candidate otherwise. That second check is a local narrowing against a replaced binding, nothing more.
10. **Fence before publication.** While the state machine holds its state guard and before a state that changes or removes the root check becomes visible (snapshot install, the establishing apply, startup recovery), it calls a synchronous fence on the crypto manager that removes a `Verified` binding of another id and raises `fenced_index`. No I/O and no await under it; a displaced binding is destroyed outside the critical section; a poisoned lock refuses admission.
11. **One coherent delivery.** After apply, the root check and the tenant keysets of the same applied state are delivered together with their index. Deliveries below `fenced_index` or not above `accepted_index` are ignored; `accepted_index` advances on every accepted delivery. A new or different check is verified outside the lock, with the cell already unverified, and installed only if neither index has moved. Every error leaves the member fenced.
12. **An unverified manager can do one thing:** produce the candidate check. That operation takes no key argument and returns no binding. The forming member becomes verified the way every member does, from the applied check.
13. **Pending is not mismatch.** A member that has not yet applied a check is pending. Only a completed comparison yields `Mismatch`. A mismatching member does no upload, encrypted read or key proposal, reports `RootKekMismatch` in readiness, and keeps its consensus vote; on its next start with an established store, startup is refused.

## Enrollment procedure (local-sealed)
1. Start the forming member. It creates `<data_dir>/.wendelon/local-sealed-root-kek.bin`.
2. Copy that file, owner-only, to the same path in each other member's empty data directory, and give each member the same unlock secret through its configured environment variable or file.
3. Start the other members in join mode.

aws-kms: the same with `aws-kms-root-kek.bin`; each member needs decrypt access to the key. dev-insecure: the same with `dev-root-kek.bin`, development only.

## Consequences
- A node whose root artifact is missing no longer starts. Before, it generated a new root and came up unable to open any tenant KEK in state.
- A restore target, and a replacement member, must be given the source cluster's root artifact before the restore. Before, the restored node generated a root of its own and only failed later, at the first encrypted read. The backup still carries no root.
- A directory whose storage was seeded before the node's first start is not a fresh forming store and generates no root; one must be placed.
- A member cannot join before the forming member has established the root check; a join in that window is refused retryably.
- After a keyset change (tenant creation, rotation, retirement) a node starts with empty key caches, as it already did after a keyset snapshot sync.

## Trust and custody
- The root is a cluster-wide secret. A member compromised while unlocked yields a key that opens every tenant KEK wrapped under it: those in state, those retained in older logs, snapshots and backups, and those installed later, until the root is replaced. Root rotation does not exist.
- Every member holds the same unlock secret; node-specific seals are not implemented. Keep the unlock secret apart from data backups.
- Root2 role-key custody is separate and unchanged; its contract excludes node root KEK custody.

## Algebra
- Check identity = (version, wrapped bytes). Equal roots give equal bytes (AES-KWP is deterministic), so the id is a function of the root and the version, and of nothing else: not the node, the directory or the time.
- A binding is keyed by (provider instance, check id). A tenant KEK entry is keyed by (tenant, tenant KEK id) inside one binding; a slab DEK entry by its existing cache key inside one binding. No key is reachable by a narrower key than its binding.
- Delivery order is the applied log index. Establishment is idempotent on the value, not on the command.

## Compatibility
Pre-release: formats are bumped in place and older stores and backups are refused at open, never converted or reset. Format numbers are never reused across modes (16 to 18 were ParentDecisions versions):

| Artifact | Before | After |
|---|---|---|
| Today WAL and snapshot | 15 | 19 |
| ParentDecisions WAL and snapshot | 18 | 20 |
| ParentDecisions store marker | `parent-decisions-v10` | `parent-decisions-v11` |
| ParentDecisions Raft and forwarded-proposal frame | 11 | 12 |
| Control backup | 1 | 2 |
| Today Raft frame | 1 (the one HRFT envelope version) | 13, its own |
| Every other Today frame (data, admin, join) | 1 | 1 (unchanged) |

The Today Raft frame gets a version because the changed request bodies travel in Raft RPCs as raw encoded bodies, and a log format number is no boundary on the wire: an earlier Today peer is now refused before decode, in both directions (Codex 01a1d423). Request tags are append-only: `EstablishRootCheck` is the new last variant. `CreateTenant` and `InstallTenantKekVersion` each gain a trailing root check id; state gains a trailing optional root check. Join refusal reason 12 (`FormationIncomplete`, retryable) is new. Unchanged: tiered object format, tenant KEK and slab DEK wrapping, provider artifact formats, configuration schema, Root2, owner-signed slab operations, placement and membership rules.

## What this does not do
- No member replacement and no backup or restore workflow: it supplies their key prerequisite only.
- The check does not certify recoverability of a backup, and replaces no Root2 authority check.
- Tenant-key lifetime across an upload's publication is a separate, open obligation (DC-231: a tenant KEK can be retired between an upload's encryption and the commit of its pointer; from source, not reproduced). This ADR neither fixes nor depends on it.
- No root rotation, escrow, node-specific seals, export or import tooling, online root transfer, per-member wrapping or KMS-direct wrapping.
- DC-224, DC-223 and the memory acceptance are unchanged. No three-host run.

## Acceptance
1. Reducer: establish, identical replay, conflict, after-keys refusal; tenant create and tenant KEK install refused with no check and with another id; replay and snapshot keep the check; version corpus for both modes.
2. Ordering: after formation, and after a restart at each interruption point (root persisted; first log write; establish outcome unknown; check committed; verified), state holds one authoritative check value with one effective establishment and no tenant KEK at a lower log index.
3. Open rules, one test per case on real data directories: generate; adopt; enroll; missing artifact on a joiner; missing artifact on an established store; wrong unlock secret; corrupt artifact; recovered check that the provider does not reproduce.
4. Authority: a two-voter state with no check establishes nothing; a join is refused before the check and admitted after it.
5. Cached keys: after a different check is delivered, a read that was a cache hit, and a new plan, are refused; the control with an identical check keeps serving.
6. Interleaving, one barrier: an operation admitted before a binding change finishes; a new operation sees only the new binding; an upload admitted before is refused at `still_current`; at a barrier placed after snapshot publication and before delivery, a new admission already refuses.
7. Receiver ordering: a stale same-check delivery does not replace newer keysets; a delayed verifier does not install over a later fence.
8. Entry points: tenant create, default seeding and tenant KEK rotation propose nothing while unverified or mismatched.
9. Three-node cluster: a member enrolled with a different root joins, keeps its vote, reports `RootKekMismatch`, uploads nothing and refuses an encrypted read, while the other two offload. The ADR 0725 offload tests pass on artifacts copied by the enrollment procedure, with the test-only common root removed.
10. Real processes: the three-daemon smoke test on local-sealed artifacts copied by the procedure, once.
11. Architecture pins: one constructor of `EstablishRootCheck`; no key function outside the binding; no stored read plan.
12. Mutants, each failing a named test: drop the reducer's id comparison; let an established store regenerate; let verification accept any provider; skip the fence at snapshot publication.
13. One shared checkpoint on the candidate. A failure is reported as failed.

## Limits
All key material in tests is synthetic and generated per test. No real key is copied, no store is reset, nothing is pushed before a delivery ruling.

## Validation record
Development evidence on bigfoot, working tree before the commits; logs in `~/work/reviews-logs/adr0726-dev/`. None of this is a checkpoint.

- **Three full development runs, all FAILED**, kept as the record of what the change broke and why:
  - t1: 402 of 4,585 failed. Chiefly my own wrong formation condition (it read a normal single-member start as "another member was admitted").
  - t2: 104 of 4,600 failed. Fixtures that joined or restored without the cluster's root, pinned format numbers and stored bodies, and ParentDecisions refusing a directory that held an enrolled root.
  - t3: 94 of 4,630 failed. The other three setup censuses refusing the same directory (decision 7a), fixtures not yet enrolled, and two restart tests whose storage verification met the new "not verified yet" refusal as a permanent error (decision 7b).
- **Focused run after those fixes (f2):** 1,347 of 1,348 passed; the one failure was a late-joiner fixture without the root, since corrected and passed (m6). It included every group that had failed, the new tests, both three-node workflow tests and the startup fault matrix with its new stop points (`root_kek`, `root_check`, `root_check_established`).
- **New tests:** root check encoding and refusal of other versions; unverified and mismatching managers; cached-key refusal with its control; the one-barrier interleaving in both variants; stale and delayed deliveries; the fence; a poisoned cell; provider open rules for the development and local-sealed providers; the artifact-directory classifier; reducer write-once and ordering rules in both modes; coherent delivery; snapshot publication barrier in three cases; node open rules (generate, adopt, missing, other root, joiner without root); the establishment routine's authority; join refused before the check; a three-node member with another root.
- **Mutants, each run against its named tests, then an unmutated control (m1 to m5):**
  - reducer id comparison dropped: `no_tenant_kek_enters_state_without_its_root_check` failed;
  - established stores allowed to generate: `an_established_store_never_regenerates_a_missing_root` and `a_joiner_without_the_cluster_root_refuses_before_any_network` failed;
  - verification accepting any provider: six tests failed, among them the mismatching three-node member and the established store with another root;
  - fence skipped at snapshot publication: `a_snapshot_of_another_root_fences_before_its_state_is_visible` failed.
  - The architecture pin also failed under the fourth mutant but was failing on the unmutated tree at that moment (a formatting change had split the line it matched), so it discriminated nothing there; it was corrected and passes (m6, 259 of 259 across both architecture binaries).
- **The real three-daemon test FAILED on the first committed candidate** (e6027884a, pd-smoke-20261113T085327Z): the forming daemon aborted with a worker stack overflow. The same test failed the same way on main without this change and passed on the commit before ADR 0725, so the cause was ADR 0725's, not this ADR's (DC-232); it was fixed first, as ADR 0727, and this work was rebased onto it.
- **Source review r1 (Codex 01a1d423) held three points, all corrected:** the self-binding fixture helper is private (it was public on a manager a node exposes); Today's Raft frames carry version 13 (the changed bodies had kept the shared version 1, so an earlier peer would have decoded them); the snapshot-publication test hook belongs to the one state machine (it was global), and the test shows another state machine's installation neither runs nor consumes it. Also in that delta: the establishment routine reads state in place instead of holding a copy across an await. Focused checks for the corrections (c8): 352 of 352.
- **Rebased on ADR 0727** (main 272895a40); one conflict, in the three-node workflow fixture, resolved by keeping both changes. Focused run on the rebased tree (r1): 1,692 of 1,692. Source r2 cleared by Codex 01a1d45b.
- **The real three-daemon test PASSED, once,** on impl a10971f3fa8ee66c0dabd7b00b2db622e0576b8f (tree 6386d69360bdb950a9f907df97372502f63b9549), unchanged launcher, debug build: three daemons formed by the enrollment procedure (the forming node's sealed root file copied to the two joiners, one shared unlock secret), served the client's write and survived a follower restart. bigfoot `~/work/reviews-logs/pd-smoke-20261113T100203Z`, seal 120af1e194ab82ca5ac8c1b7e6df10f72cbeba5c92c7f14a7cb9fcd3b979c0ec. It does not show real daemons offloading, or a three-host cluster.
- **The shared checkpoint FAILED: 4,608 of 4,610** on that same tree (existing recipe, bigfoot `s2-decisions/keep/adr0726-gate1.log`, sha256 fd06cc87f75869c01c5b33aedcdb4a16f400521f6d4543aac988c57a7663c4a5; fmt and workspace check clean). Both failures are in `consensus::retention::tests` (`full_refuses_snapshot_request_before_decode`, `retention_status_is_additive_and_shared`): their fixture builds a Today Raft frame with the envelope version written as the literal 1, which Today's Raft handler now refuses before the retention guard the tests are about is reached. A test-side consequence of giving Today's Raft frames version 13, and a gap in my focused selections, none of which included the retention tests. Corrected in the fixture (it asks the mode for its Raft version); the retention tests then pass, 13 of 13 (ret1). **No checkpoint has run on the delivered tree**, which differs from the checkpointed one in that one test fixture line and this record. Both three-node workflow tests and the DC-222 test passed in the checkpoint: one sample each, no claim about DC-222 or DC-224. Not tested at all: a recovered state holding tenants but no root check (refused at startup by source; the reducer makes it unreachable), and the aws-kms provider's enrollment (no KMS in the test environment).
