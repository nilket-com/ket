# ADR 0738: `nilket local init` creates a fresh single-node ParentDecisions profile on request

Status: proposed (2026-11-19 UTC). The first unit of the cutover to ParentDecisions as Wendelon's sole serving decision authority (operator priority, 2026-11-18). Builds on ADR 0721 (a daemon's configuration selects the authority) and wendelon ADR 0104 (formation consumes a provisioned intent). Separate from ADR 0172 (the local profile in a Podman container), which it does not change.

## Why
- Every normal entry point selects Today. Real-daemon serving checks under ParentDecisions already exist, through explicit hand-written configs (`wendelon-client --test parent_decisions_processes`). What the normal local entry point lacks:
  - nothing generates a decision signing key or the trusted-signer map;
  - only tests write the `FORMATION_INTENT` a fresh store needs;
  - the default MVCC window (7 days) is refused under ParentDecisions.
- This adds the normal local provisioning entry point. A one-voter ParentDecisions store admits its first write: it forms with its intent, commits a primary-key write, reads it back exactly, and reads it again after a clean stop and a restart without an intent (wendelon `one_voter.rs`).

## Decision
1. **An explicit request, at init only.** `nilket local init --decision-authority today|parent-decisions`. With no flag, nothing is asked: a new profile is Today, and an existing profile keeps what it was created for. `nilket local` (run) takes no authority.
2. **The profile's required marker holds the authority and its formation stage, bound to its UUID.**
   - **The field:** `ProfileMarker.decision_authority`, either absent or `{ authority = "parent-decisions", formation = "initializing" | "issued" }`. The serialized form makes an invalid pairing unrepresentable: an unknown authority, a missing or unknown stage, or an extra key refuses as a malformed marker. Today and legacy markers keep their exact bytes.
   - **No sentinel:** there is no separate record file. An issued identity is never inferred from a missing file.
   - **The opt-in creates a new profile only** in a new, empty `--data-dir`. Refused, before anything is written:
     - an existing profile without a recorded authority, whatever its config says, since an existing store is never adopted or converted;
     - Today requested on a ParentDecisions profile.
   - **Legacy:** a profile without a recorded authority keeps its previous behaviour: an operator-edited config is accepted as before, no key or intent is issued, and the daemon's own store checks apply at startup.
3. **The config.** `wendelon::config::render_parent_decisions_local_profile_config` is the local-profile config plus four changes:
   - `cluster.decision_authority = "parent-decisions"`;
   - a fresh OS-random decision signing secret, never a development key;
   - a trusted-signer map holding this node's verifying key only (a single-node example, not fleet provisioning);
   - an MVCC window of 0.

   Rendering writes nothing and grants no formation.
4. **State before generation.** A ParentDecisions profile's state is read before any write:
   - **Its real directories** (a symlink or a non-directory refuses).
   - **Its files:** the config, the intent, the marker's republication file.
   - **Its store:** anything in `data/wendelon` but `LOCK` and the intent.

   Enumeration and inspection errors are errors, never "empty". The marker's stage allows exactly one plan, and every refusal precedes every write: no key, PKI, config, directory or intent is created for a refused profile.

| stage | condition | plan |
|---|---|---|
| `initializing` | store present | refuse |
| `initializing` | config missing, intent present | refuse (the key is never regenerated) |
| `initializing` | otherwise | provision: render the config only if absent (no daemon has ever run), provision the intent if absent, sync every prerequisite, publish `issued` |
| `issued` | config missing | refuse |
| `issued` | store present | ready (formed) |
| `issued` | intent present | ready (unconsumed): sync the prerequisites again, then start |
| `issued` | neither intent nor store, including a lost `data/` | refuse: the profile never forms again |

5. **The managed config is checked.** It must select ParentDecisions, form a new cluster (`cluster.mode = "bootstrap"`), and keep its store exactly in the profile's own data directory, where the intent is provisioned.
6. **One durable, ordered issuance.**
   - **Order:**
     1. the marker at `initializing` (create-new, synced with its directory entry);
     2. the profile's own directories (each new entry synced into its parent);
     3. the config holding the key (create-new, synced);
     4. the intent (Wendelon's create-new, synced with its directory).
   - **Publishing `issued`:** the files and directory entries above are synced again, since a previous interrupted run may never have synced them. Then the marker is republished at `issued`, same UUID: a create-new temporary file is synced and renamed over the marker, and the root is synced.
   - **The ready path:** a profile at `issued` whose intent is still unconsumed syncs those prerequisites again before the daemon is started, so a previous run stopped before its final syncs is never passed over. A formed profile does no syncs: its store exists only because a daemon started after a durable `issued`.
7. **A retry differs from a reset.** Re-running `init` (with or without the flag) or `nilket local` never regenerates a key and never reissues an issued intent. Only the explicit `nilket local --reset` returns an `issued` profile to `initializing`:
   - **Order:** it checks the marker's republication file before any destructive step, discards the store, then republishes the marker at `initializing` (same UUID).
   - **Outcome:** a new store, a new key and a new intent follow.
   - **If interrupted before the republication:** the profile stays `issued` without a store and refuses until the reset is repeated.
8. **Tests on the fixed local ports run alone.** The five ignored live local-profile tests bind the fixed public ports (7001, 7742, 7431 to 7433). They run in nextest group `live-local-fixed-ports` (allowlisted with its reason); every other test stays parallel. Register entry PD-29 records the collision that showed this.

## Durability guarantee, stated
- **Guaranteed:** after a host crash at any point, preparation either resumes a provisioning that never reached a durable `issued`, or refuses. A key is never regenerated, and an issued intent is never issued again.
- **The rule this rests on:** a daemon on the profile is started only by `nilket local`, after a durable `issued`. A daemon started by hand on an `initializing` profile is outside this guarantee.
- **What is not covered:** the store's own durability (Wendelon's), and the profile's PKI and Toron config, which are regenerated when absent, as before.

## Limits
- One node, rf 1, a development PKI, and a decision key held in the config file. This is not production custody and not fleet provisioning. The standalone `--print/--write-example-config` is unchanged.
- **The default stays Today** until the remaining client capabilities are accounted for. Under ParentDecisions, Toron's DROP TABLE, CTAS, multi-row DELETE, mutation-outcome resolve, query cancel, reading after ALTER, and counters stay refused (MAP-01).
- Existing deployments and their data are a separate operator decision. Nothing here touches a store it did not create.

## Acceptance
- **The one-voter first write** (wendelon `parent_decisions_cluster_tests::one_voter`).
- **The `nilket` unit controls** for every stage, interruption point and refusal: each refusal leaves every byte of the profile and creates nothing. They include:
  - **The issued profile whose `data/` was moved aside** (marker, config and key kept), refused by init with and without the flag and by the run path, with no intent and no directory recreated. Also: a formed store lost; the whole `data/` lost.
  - **The positive cases:** a fresh profile, and every `initializing` interruption point, provision without regenerating a written key.
  - **Also:** damaged managed markers; the reset round trip and its pre-destruction check; the default and explicit-Today init; a legacy profile with a ParentDecisions config; CLI parsing.
- **The public entry point, live:** `local_profile_parent_decisions_init_commits_reads_and_survives_restart`:
  1. `init --decision-authority parent-decisions`, then `nilket local`;
  2. `CREATE TABLE … PRIMARY KEY`, then `INSERT` answering `OK rows=2`;
  3. the exact ordered rows and count, compared by a row oracle with its own negative controls;
  4. a stop, then a re-run of init (no key or intent change), then a restart;
  5. the same exact rows.
- **Run together:** the five fixed-port live tests in one nextest invocation; the architecture fences; formatting.
