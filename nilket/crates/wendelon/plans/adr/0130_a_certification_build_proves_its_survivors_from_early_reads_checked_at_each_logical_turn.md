# ADR 0130 - A certification build proves its survivors from early reads, each checked at its logical turn

## Status
Proposed (local), 2026-11-25: the plan of a **local v26 implementation candidate** for the decision maintenance pass's certification build. Not a delivery decision.

- **Per Codex:**
  - 01a21236: the build's cost to be isolated on delivered v26, test-only;
  - 01a2124e: the attribution accepted; the survivor proofs selected as the repair target; my "32 decodes" prediction corrected (four frame entries, logical visit order);
  - 01a21265: the same-order comparison accepted as a non-result; a comparison with the proof reads grouped by child and frame, repair, first refusal and stop kept in logical order;
  - 01a2127e: bounded successive logical windows; discard after repair; the correctness boundary first;
  - 01a212ae: validity across a change from outside the build to be established before any candidate;
  - 01a212c5: validation by witnesses of the actual per-copy reads, within the existing read path;
  - 01a2131c: GO for this plan and candidate, with one implementation delta (eligibility is decided at backing selection, before any read);
  - 01a2133e: this decision takes the number 0130.
- **Base:** origin/main `9debddadb`. The held bounded-install draft ("a certified base is published by renaming its staged table, and its retirements settle in bounded pieces", local `a805fe938`, called ADR 0130 in earlier review records) is not delivered and takes the next free number when it lands; nothing here depends on it. This decision was drafted as 0131 and renumbered to the next number on main (per Codex 01a2133e).
- **Evidence** (a real one-voter ParentDecisions store, AppendOnly MultiRow, a 16-column approximation of lineitem, release, tmpfs lane, one run per arm, test-only probes off main; packets DC290-BUILD-COST-PROBE, -REUSE-COMPARISON, -GROUPED-COMPARISON, -WINDOWED-FOLLOWUP, -EXTERNAL-CHANGE, -WITNESS-VALIDATION):
  - a build of 32,768 survivors in 32 frames spent 10,210 ms of 10,429 ms proving survivors: **32,768 whole-frame reads and decodes for 32 frames**; the manifest lookups read no frame;
  - the delivered four-frame set in the build's own (logical key) order thrashes: 28,567 reads, 11 % less time. Keys are hashes: the next proof falls in the previous one's frame 1,054 times in 32,768;
  - with only the read order changed (by source child, then by location), checked at each turn as decided below, 40,960 survivors in 36 frames at a **1 MiB** scratch bound: **396 reads, 1,611 ms of proof work against 40,960 and 14,135 ms**; children of 17 to 19 frames: 1,259 reads and 1,353 ms against 32,768 and 4,225 ms;
  - the same candidate computed twice from the same inputs, once each way, is equal entry for entry (HLC, position, kind) with equal chunk sums;
  - **an early proof used without a check is wrong**: with the store-wide retirement fence raised, a local store removed from under an unchanged index, or a second copy changed with the first unchanged, the ordinary build refused and a build using saved proofs unchecked staged all 40,960 entries. The check decided below gave the ordinary outcome in all three.
- **Not claimed:** NEXT7's memory rise during its first build is not attributed, and its frame reads are not attributed to this path by the seal. Timings are a fixture's. This is not small-node qualification.
- **Held:** publication (the bounded-install draft and its D3), delivery, measured Section 8 runs. DC-290 and DC-296 stay open.

## Context
- `build_candidate` (`network/data_server/certification.rs`) settles the candidate's winners, then proves every winner that comes from a child, in the winners' logical order (tenant, table, key identity). A proof is `proved_location_in` (`network/data_server/reader.rs`): list the version's same-HLC copies under the authority admission; read every copy (`stored_rows_at_version`, which lists again and reads each copy through `resolve_read_target`); require every copy's row digest to equal the recorded one; return the first listed location and the version's locality.
- Each copy read reads and decodes its whole frame, and nothing is kept between copies. Key identities are hashes, so the logical order is unrelated to where rows are stored.
- ADR 0128 gave a holder's verification a set of at most four decoded frames under a shared per-node allowance. It serves a walk that stays within one child's frames. The build's walk does not: it crosses every child at random.
- The build holds one lifecycle permit, which delays a close. It excludes no writer. While it runs, the authority admission can change (the retirement fence is store-wide and is evaluated on every listing), a version's copy set can change (an append, a relocation by LSM, maintenance or tier refresh, a retirement, a repair), a copy can come to resolve elsewhere (a redirect, a tiering, a rehydration, an eviction, a quarantine) and a conflict mark can appear. The ordinary proof asks all of this at the survivor's own turn.

## Decision

### Scope
- **Changed:** when the frames of a child-sourced survivor's stored copies are read and decoded, and through what: early, a bounded window of the logical order at a time, in an order that meets each frame about once, through the node's verification frame set.
- **Not changed:**
  - the winners and their order; the logical loop visits every winner in logical order and alone observes the stop at a turn, proves an inherited winner (a recovering replacement), repairs a child once, refuses, and sets a position;
  - what a proof requires: every listed copy's digest; the same answer (first listed location, locality);
  - what is asked at a survivor's turn: both listings with their admissions, and each copy's resolution;
  - a recovering replacement's build (`node/replacement_recovery.rs`), which proves each survivor at its turn as today; a holder's verification and every other caller of the frame set;
  - any format, lock or pin. Stock redb and v26.

### D1. Windows of the logical order
- The loop takes the winners a window at a time: the next N winners in logical order, N fixed for the build by D5. With no early proofs there is one window, the whole candidate, and the loop is today's.
- For a window, before the loop visits it:
  1. each child-sourced winner with a stored data copy is scheduled by where its first listed copy lies (slab, offset). The listing is a scheduling hint and proves nothing; a refusal, an unlisted version or the typed tombstone case schedules nothing (a typed tombstone is proved by its listing alone: there is no read to move);
  2. the scheduled winners are sorted, in place, by (source child, slab, offset) and proved in that order through one frame set (`slab.verifier_frames()`: four entries, the shared allowance of ADR 0128, no retirement wait);
  3. a proof that succeeds is **saved** with its witnesses (D2). Anything else saves nothing: a refusal, a missing or differing copy, a version that is not eligible (D4) or does not fit (D5).
- Then the loop visits the window in logical order. A winner with a saved proof is checked (D3); a winner without one is proved by the ordinary proof, exactly as today.
- The frame set lives for one window's early proofs and is dropped before the loop visits the window. The window's vectors are reserved once and reused.

### D2. A witness per copy, from the read that supplied its digest
- An early proof's reads are made by `stored_rows_witnessed` (`storage/manager/witnessed_reads.rs`): the data path's own listing, then each listed copy in order through `resolve_read_target` and the frame set.
- The delivered `stored_row_reusing_frame` is split at its resolution: `stored_row_resolved` takes the first resolution from its caller. The holder's path calls it with none and is unchanged: no counter, toggle or allocation is added to it.
- For each copy the witnessed read keeps, from the resolution it then reads through (so on a frame hit as on a read):
  - the listed location it was asked for;
  - the local store that serves it, by a weak pointer (it names the store and owns nothing of it, as a held frame does);
  - the resolved target location: the frame's extent, the row, the HLC and the kind, which is every input `verify_entries_name_version` compares for a local frame (its expected tenant and key are unset for a local store);
  - whether the frame was verified to name the version (the slab was tiered at the read).
- A saved proof holds the ordinary answer and its copies' witnesses in listed order.

### D3. The check at the survivor's turn
- After the turn's stop check and the inherited-winner branch, a saved proof is checked by `version_resolves_as_witnessed`, which reads no frame:
  1. the reader's listing (`version_locations`: the authority admission and the read scope). The first listed location must be the saved one, and the list must not be the all-tombstone case;
  2. the data path's listing (`same_hlc_locations`: the admission again). It must list exactly the witnessed locations, in order, each equal in slab, offset, length, HLC, row index and kind;
  3. each listed copy through `resolve_read_target(location, true)`: its redirect, read-scope wait, pin, backing snapshot, target scope, conflict mark and row-index rule. The backing must be a local store and the same one (pointer identity with the witness's weak pointer, which keeps the address from being reused); the resolved target must equal the witnessed target whole; and the slab must not be tiered now unless the witnessed read verified the frame.
- **A refusal** of a listing or of a resolution is the ordinary proof's refusal at that turn. It is returned through the ordinary proof's own mapping, and the ordinary proof is not then tried: a second attempt after a live refusal could succeed where the ordinary proof, made once, refused.
- **A difference** drops the saved proof, and the ordinary proof runs with its own repair and refusal.
- **A repair** issued by the loop drops every early proof still saved, before its second proof: none made before a repair is used after it. They are proved by the ordinary proof.

### D4. Eligibility: a live local store's published extent
- Only a copy that resolves to `SlabBacking::Local` is read early. The decision is made at the backing selection, before any read: a copy that resolves to a rehydrated copy, an L2 object or no slab is not read, no missing reference is accounted for it, and no L2 retry is made. Its version is not saved, as a whole; the ordinary proof at its turn does all of that as it does today.
- What the check rests on: **the bytes of a published extent of a live local store do not change.** Source, on the base:
  - an append reserves a fresh offset: `next_offset` is set at construction and otherwise only advanced by `fetch_add` (`storage/core.rs`);
  - a failed append zeroes its own reservation on the write task before it reports the failure (`storage/core.rs`, ADR 0099 decision 2). That is before the append returns, so before any index entry names the extent: a published extent is one whose append returned and was indexed. An index rollback (`rollback_index_entry`) removes an entry and no bytes;
  - the slab store has two positional writes, both in that append (the payload, and that zeroing). No `set_len`, `ftruncate` or `fallocate` reaches a slab file: `set_len` is used by the whole-file writer (`storage/file_ops.rs`: restore artifacts before a store is opened, raft storage, local L2 objects through a temporary file and a rename), the counter files and the root2 owner log;
  - recovery may zero unacknowledged regions when a store is opened, before it serves;
  - a local store leaves service by leaving the backing map (tier publication, retirement, compaction without replacement, quarantine). The check then resolves the copy to another backing or to none, and the saved proof is dropped;
  - slab ids are not reissued.
- A rehydrated copy's lifetime and an L2 object's not-found re-resolution are not established here, so those backings are not eligible.

### D5. Bounds
- **Scratch: 1 MiB per build** (`PROOF_SCRATCH_BYTES`), reserved once at capacity before the first window and never grown: a window's slots, hints, saved proofs and witness arena; the one-version witness sink; and, at what a weak pointer keeps allocated (`WEAK_STORE_BYTES` plus a word), one store identity for each of the stores a window may name and for each witness that may wait in the sink.
- **A version: at most 8 copies** (`VERSION_COPIES`). A version listing more is not read early at all.
- **A window: at most 16 distinct local stores** (`WINDOW_STORES`) and an arena of nine eighths of a witness per winner. A proved version whose witnesses do not fit, or that would name a seventeenth store, is not saved.
- In each case the version is proved by the ordinary proof, as a whole.
- **Decoded frames:** the delivered four entries and shared 8 MiB allowance of ADR 0128, unchanged, charged apart from the scratch. A frame that cannot be kept is read and dropped, which is the ordinary read.
- N follows from the scratch bound and the type sizes; in the probe 3,742 winners a window.

### D6. Stop
- The stop is observed before each scheduling listing and before each early proof, as well as at each logical turn. Seeing it, the build returns nothing to report (DC-296), with nothing staged.

## Tests
- On a real one-voter store through the ordinary maintenance pass (`node/parent_decisions_cluster_tests/survivor_proofs_tests.rs`), with test-only counts of which path proved what:
  - the candidate equals the one computed from the same inputs with every survivor on the ordinary path; more than one window; every survivor proved early and used; each window reads a frame at most once; the scratch within its bound; no frame left charged;
  - held after a window's early proofs: the production retirement fence raised; a sealed slab's local store removed with the index unchanged; a second copy changed with the first unchanged. Each compared with a second store whose builds prove on the ordinary path, changed at the equivalent point: the same refusal text, nothing staged, and the counts showing the saved proof refused or rejected;
  - a version of nine copies and more two-copy versions than the arena holds: proved on the ordinary path as wholes, the candidate still equal to the ordinary one;
  - a differing later copy; a version with a copy in a slab whose store is gone at the early proof: not saved, no missing slab accounted early, the ordinary refusal;
  - a stop between two early proofs: nothing staged, an orderly close.
- On three nodes: a leader that lost its logically first survivor repairs its child from a peer, drops the early proofs made before the repair, and the generation commits.
- The delivered tests of the build (the drain controls, the recovered-candidate rebuild with a repair) and of the holder's frame set run unchanged.

## Rationale
- The cost is the order, not the size of the set: by source child and location, four frames meet each frame once, at 1 MiB of scratch.
- Moving only the reads keeps every decision where it is. The alternative, moving the proofs, would change which refusal is first and which child is repaired first.
- A saved result is only as good as the bytes it stands for. The check asks, at the turn, everything the ordinary proof asks except to read the frames again, and the one thing it does not re-ask is covered by an invariant the source supports for one class of backing. Everything outside that class stays ordinary.
- Windows keep the scratch fixed whatever the candidate's size, and bound how long a saved proof waits for its turn.

## Consequences (The "Negatives")
- More code on a certification path: a window, witnesses and a second way to reach the same answer. The ordinary proof remains the reference and the fallback.
- A build may read a frame more than once: once per window that touches it, and again for every survivor that falls back.
- A candidate whose survivors are mostly tiered gains nothing: their versions are not eligible.
- The check costs a listing pair and a resolution per copy at each turn (7 to 8 µs a survivor in the probe), which the ordinary proof pays too.
- A weak pointer in a witness keeps a dropped store's allocation until its window ends, at most sixteen stores a window, counted in the scratch.
- While a window's early proofs run, the logical loop is not visiting: a stop is still observed between proofs, but a refusal that the ordinary order would meet at once is met after the window's early proofs.

## Open Questions (Optional)
- The gain on a store on disk, with an inherited base, under concurrent writes and at small-node memory is not measured.
- Whether a rehydrated copy can be made eligible.
- Whether NEXT7's memory rise during its build has anything to do with this path.
