# ADR 0118 - A ParentDecisions SQL DROP succeeds at its logical finalize

## Status
Accepted as the design of one release unit, 2026-11-20.
- **Operator:** "go ahead with the next DROP unit design working with codex". The operator's direction is 01a1f7c8.
- **Codex:** DROP-21 01a1f7f5, DROP-22 01a1f7f7.
- **Held:** implementation, push, SQL enablement and physical settlement remain held until the unit is reviewed.

**One release unit:**
- logical DROP under ParentDecisions;
- permanent table-id protection;
- logical backup and restore.

Internal commits may land separately inside the unit. **The public refusal (`control_server.rs:3287`) is removed only by its last commit, and no intermediate protocol version is published.**

## Context
**Today mode already replies to SQL DROP at LogicalFinalize** (`control_server.rs:3291-3310`); reclaim and settlement follow later. **Under ParentDecisions, DROP is refused twice:** at the control server (`UnsupportedUnderParentDecisions`) and at apply (Prepare without the test admission).

**ADRs 0114–0117 built the PD drop under test admission:**
- a Prepare that concludes Reserved parents;
- the excluding generation (survivor policy 2);
- settlement and legacy reclaim refused;
- debt retiring only on an authoritative absence.

**Enabling it needs four further things:**
- **One replicated rule.** The admission is a test-only, member-local input, and `propose_target` compares the survivor policy at apply.
- **Permanent id protection.** Create checks the name-bound descriptor and `latest_by_id`, never the permanent outcome.
- **A usable backup.** The roster refuses while any fence, tombstone or debt exists, and refuses an unnamed descriptor (`logical_backup/roster.rs:131-137, 179-186`). Under PD the tombstone, the debt and the unbound descriptor persist, so one DROP would block every later backup.
- **A safe completion.** toron completes a DROP by resolving the **name** again (`coordinator.rs:2937`, `catalog.rs:4017-4044`).

## Decision

### D1. One replicated PD drop rule
- The test admission is removed: `pd_drop_admitted()` is "is ParentDecisions", and `StateAdmission` disappears.
- `pd_finalize_indices` is a production `serde(skip)` field, derived and validated at load, recovery and install.
- 0114's PD Prepare (the backup-hold check, the premises, concluding Reserved parents as `Refused { TableDropped }`) is the PD Prepare.
- Every participating member applies the same rule.

### D2. The version fence: four independent PD boundaries
**Bumped in place, pre-release, as ADRs 0112/0113 did for the frame:**
- the PD mode marker (v20 → v21);
- the PD WAL format;
- the PD snapshot format;
- the PD Raft and forwarded-proposal frame version (23 → 24).

**Today's wire and disk bytes are unchanged.** A frame of the other PD version is refused before its body is decoded, in both directions, for Raft append and vote, install-snapshot, and forwarded decision proposals with their answers. **A previous-format PD store refuses to open without mutation.**

**The upgrade route (ADR 0736 eligibility):**
1. export on the old binary;
2. a verified restore into a fresh cluster on the new binary.

That preserves the supported live rows, schema and settings. **Write outcomes, version history and retry identities do not cross into the new installation.** An ineligible old store has no demonstrated route here. The source store is preserved, and any eventual cutover must account for writes after the backup cut. **No destructive conversion is part of this unit.**

### D3. Permanent id protection (ADR 0115 D4)
Under ParentDecisions, a table `Create` apply refuses an incoming `(tenant, table_object_id)` that has any `TableDropOutcome`, with no effect. A fresh id under the former name, and the same id under another tenant, are unaffected. Today mode is unchanged.

### D4. The SQL reply at LogicalFinalize; cleanup deferred
**The reply:** the control path proposes Prepare, then LogicalFinalize, and replies at its apply, with no wait for the excluding generation. **A retry with an existing fence of the same operation resumes** (`control_server.rs:3275-3282`). The sweeper's roll-forward completes an abandoned Prepare.

**Physical cleanup stays deferred.** The claim covers **only the evidenced path:** the excluding generation (0115), activation debt, and certified cleanup of local copies (0117). Settlement and legacy reclaim stay refused (0116), and tier and L2 cleanup are not claimed.

### D5. The backup roster: an exact exception, and typed hold retries
**The roster skips `(tenant, T)` only if every one of these holds:**
- an outcome, a tombstone and a debt all exist, with **equal tenant, table, `operation_id` (including `schema_generation`) and finalize index**;
- the tombstone's and the debt's `fence_log_index` are equal;
- the finalize index is at or before the roster cut;
- T's descriptor at that generation is present in `by_id` **with no name bound to it**;
- no fence for `(tenant, T)`;
- **no `table_lifecycle` record and no materialization certificate for T;**
- no secondary index (live or dropping) referencing T.

**Any other combination of these keys refuses as today.** Both roster reads and the bounded hold release are unchanged.

**The hold orderings, each with its own typed result:**
- **Hold first:** the PD Prepare is refused at apply and has no effect.
- **Prepare first, then the hold:** the hold's roster read refuses while the durable fence stands, as a bounded refusal, and the hold is released. LogicalFinalize under an active hold is refused at apply under PD. **The original operation can still roll forward after the release.**

**The public answer:**
- These refusals are new `WendelonResponse` variants, **appended** to keep existing serialized tags. `submit_table_drop_stage` maps them to a typed, retryable `WendelonAdminError` that names the blocked stage, no longer to `Transport`.
- **The client never reports a hold-blocked DROP as known to have had no effect,** and never retries a freshly bound SQL name.

### D6. Completion by captured identity (toron)
**The durable outcome is keyed by tenant, old object id and generation.** toron completes the **originally bound** statement by that captured identity:
- `mark_finalized_drop` and the final removal become one registry operation under one write lock: compare that the local entry still names `(table_object_id, schema_generation)`, consume the marker, remove.
- **It never resolves the name.**
- **After an authoritative storage success, a missing or rebound local entry leaves any fresh binding intact and lets the original operation finish.** A missing local entry alone is never taken as evidence of durable success.

**The retry contract:**
- **The identity-bearing retry entry point** is the admin request `drop_table_schema(TableSchemaDropRequest { operation_id, expected_generation, … })`.
- **Within a toron process** the coordinator holds the identity in the statement's captured binding.
- **After a toron restart,** a newly submitted SQL `DROP TABLE name` resolves the name afresh (`coordinator.rs:2915`):
  - if toron's registry still binds the name to the old object, it replays the old operation by identity;
  - if the name is bound to a recreated table, it targets the replacement, as a new statement.
- **The SQL guarantee is therefore completion of the originally bound statement.** A generic SQL exactly-once retry token is a separate feature.

## Rationale
One release unit, because each part is unsafe alone:
- enablement without D5 blocks every backup;
- without D3 a dropped id could be reused;
- without D2 two versions could apply Prepare differently;
- without D6 a completion could misreport or touch a recreated name.

## Consequences (The "Negatives")
- **A PD store from before the unit cannot be opened;** its route is export and verified restore.
- **The dropped descriptor, tombstone and debt stay** until a settlement authority exists.
- **Backups skip only an exactly proved, finalized drop;** any other partial state still refuses.

## Acceptance (each control RED before its fix; the public entry opened last)
**The interval-control method:**
- **In process** (the wendelon crate's three-node cluster tests):
  - **a test-only start-time hold of certification reporting** (`hold_certification_at_start_for_test`, with a runtime release) keeps the excluding generation uncommitted. It survives an unclean reopen and does not stop writes, reads or activation of already committed generations;
  - **`pause_cleanup`** (`hold_cleanup_at_start_for_test`) holds cleanup;
  - each member's installed gid and cut (`base_meta().active_gid`, `installed_cut()`) are recorded at each checkpoint;
  - **DROP is driven through the client's `drop_table_schema`,** the real control path;
  - logical export and restore use the `logical_backup` APIs.
- **toron tests:** the SQL-level completion by identity.
- **The real-daemon live CLI lane:** the public SQL DROP success, added with the enabling commit.

**The controls:**
1. **D3:** a PD Create of an id with a historical outcome and no descriptor is refused (it passes the old checks); a fresh id under the same name succeeds; another tenant is unaffected.
2. **D2:**
   - mixed-version peers refuse each other before apply, in both directions, for Raft, install-snapshot and forwarded proposals;
   - a previous-format store refuses to open, with byte-identical files;
   - the new-format snapshot install and replay agree across members;
   - Today's bytes are unchanged (goldens);
   - **an eligible old-binary export restores verified on the new binary.**
3. **D1:** a pending excluding target, its install and its replay give equal states and digests on all members, with no admission input.
4. **D5:**
   - exact-match skips;
   - refusals for each mismatch (operation, finalize, fence index, generation), a fence, a lifecycle record or certificate, and a referencing index;
   - a fresh same-name binding is included;
   - **both hold orderings, with their typed public results.**
5. **D6 (toron):**
   - a delayed old completion across a name reuse leaves the fresh binding and its rows intact and finishes the original;
   - a lost reply, then a restart, replays by identity;
   - a rebound or missing local entry after an authoritative success is handled as stated;
   - access through a captured old id is refused;
   - a write straddling Prepare is never accepted.
6. **The pre-exclusion interval** (generation and cleanup held):
   - at DROP success every member's active base still holds the old t's entries;
   - a fresh t and an unrelated u are readable and writable exactly, and the old id is refused;
   - an unclean reopen of every member keeps the old descriptor and certified copies, and the gates;
   - **export and restore carry the fresh t and u exactly, without the old t.**
7. **The post-exclusion interval:** activation of the excluding generation is observed on every member, then the same checks run with cleanup held. After the release, only the 0117 local cleanup is claimed.
8. **The enabling commit:** the public SQL DROP succeeds on the live CLI lane (real daemons). **Today's DROP tests are unchanged.**
