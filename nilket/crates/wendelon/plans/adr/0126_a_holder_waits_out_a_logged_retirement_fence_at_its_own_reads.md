# ADR 0126 - A holder waits out a logged retirement fence at its own reads

## Status
Proposed (local), 2026-11-23.
- **Per Codex:**
  - 01a20990: a local holder availability defect is established across a transient fence (DC287-HOLDER-REFUSAL-TRACE §6);
  - 01a20997, 01a209a0, 01a209a9, 01a209b2: the design's amendments R2–R4;
  - 01a209b4: GO for one plan and one implementation commit;
  - 01a20a1a: R1 (no wake registration on the admitted path; one shared manager predicate) and R2 (a caller deadline shorter than the cap, controlled through `coordinate`; the wait on the absolute end).
- **Base:** origin/main `591f932cd`.
- **Evidence it rests on:**
  - the focused control `7ea523e01`;
  - the public-path baseline `41e5fa2f8`, run once: under a held real certified Retire, the leader's holder scan refused at once and the write answered `WRITE_STALLED` / INDETERMINATE.
- **Unchanged:** the attribution of the RF1 Section 8 failures stays open. The Section 8 runs, qualification and delivery stay held.

## Context
- **What a holder reads:** a ParentDecisions holder reads its own child's rows twice:
  - the pre-append missing-row scan (`child_writes.rs` `persist_locked`);
  - the post-append verification (`verify_owned_rows`).
- **How both reads are admitted:** they go through `stored_rows_at_version` → `same_hlc_locations`, which first calls `require_tier_authority_ready()`.
- **The failure:** while a certified tier Retire's apply holds the store-wide retirement fence, that admission refuses at once. The holder then refuses its own child (`Storage` at the scan, `Unverified` at verification), the parent stays Reserved with `PendingEvidence`, and the write answers `WRITE_STALLED`, although the fence clears moments later.
- **What the ordinary append path already does:** it waits such refusals out (`append_waiting_out_a_refusal`). The holder's reads do not.

## Decision
### Scope
- **Where:** the ordinary table coordinator's **local** holder, at the two demonstrated read sites only. Every child of one coordinated attempt shares **one absolute optional-wait deadline**.
- **Not changed:**
  - the admission gate, the hard errors and their priority, and the final decision authority (Finalize and the reducer);
  - the replica role, repair, remote holders, KV writes, CTAS chunks, deletes, readers, base fetch, certification and recovery validation;
  - the append between the two reads (a separate question);
  - wire, format and durable state.

### D1. The retirement class: one atomic
`TierAuthority::retirement_fence: AtomicBool` becomes `retirement_class: AtomicU8`, with these bits:
- `LOG_INDEXED`: some `retirements` entry is `Some(index)`;
- `UNINDEXED`: some entry is `None`;
- `SNAPSHOT`: `snapshot_retirements` is non-empty.

**How the class is kept:**
- **The fence is "class ≠ 0",** so every reader of the fence makes one acquire load.
- **The writers keep today's map transitions exactly,** under the existing `status` mutex:
  - an occupied `None` stays `None` on an incoming `Some`;
  - `Some(old)` advances only to `Some(new)` with `new > old`;
  - an incoming `None` replaces `Some`;
  - `clear_retirement` removes only an exactly matching `Some`;
  - the snapshot variants are unchanged.
- **Counts kept under the mutex** (log-indexed entries, unindexed entries, snapshot entries) let a writer recompute the class without scanning a map.
- **Publication order, under the mutex:** an added blocking class is OR-ed in **before** the mutation; the exact class is stored **after** it, so a removed class clears only after the matching mutation.

### D2. Admission unchanged; the deciding evaluation reports why it refused
`require_tier_authority_ready` keeps its predicate, its order, the `exposed` fast path, `expose_if_ready`'s publication and `start()`.
- **The readiness evaluation is computed once** into `ReadinessBlocks`, reading each atomic once: integrity, the retirement class (one load), snapshot fences, not ready, closed. `is_ready()` is "no blocks".
- **`tier_authority_refusal` also names the branch it selected** (`RefusalBranch`). The error and its priority are unchanged.
- **One manager predicate:** the six manager-level terms move, in their short-circuit order, into one `manager_authority_terms_clear()`, which both admissions call.
- **New: `SlabManager::holder_location_admission()`.** It runs that predicate and the same `expose_if_ready` evaluation (first exposure and verifier start unchanged), and returns `Admitted` or `Refused { error, eligible }`.
  - **`eligible` is true iff:** the manager-level terms were clear, the readiness blocks were **only** the retirement class, that class was **exactly `LOG_INDEXED`**, and the selector chose **`Fallback`**.
  - **A mixed, unindexed or snapshot class, a snapshot fence, not-ready, closed, an integrity failure, any manager-level term or any higher-priority branch is not eligible.** For example, unresolved-op with retirement gives `WouldBlock("UnresolvedOpIntent")`.
  - The refusal error is always today's.

### D3. The holder's read waits, then re-reads
**The read:** `stored_rows_at_version_for_holder(…, wait)`:
1. runs `holder_location_admission()` once, **registering nothing**:
   - `Admitted`: list the locations and run **the actual reads**, as `stored_rows_at_version` does. A ready store's holder read therefore costs what it did;
   - `Refused`, with no wait, not eligible, or the end passed: return today's error;
2. only after an eligible refusal with time left, loops:
   - create `changed.notified()`, pin it and `enable()` it **before** re-checking;
   - re-run the admission: `Admitted` reads as above; a refusal that is not eligible, or the end passed, returns today's error at once (hard errors and close stay prompt);
   - otherwise wait on `timeout_at(min(end, now + 50 ms), changed)`, an **absolute** end, then repeat.
- **Unchanged:** a later per-location refusal returns at once; the whole child is never replayed; no other read changes.

**The wait itself:**
- **Its owner is the attempt:** `coordinate` computes **one** `RetirementWait` immediately before `write_children`. It is `min(now + HOLDER_RETIREMENT_WAIT, the caller's deadline)`, **only for an ordinary table write** (a `Table` namespace and no CTAS dispatch); otherwise it is none.
- **The bound:** `HOLDER_RETIREMENT_WAIT` is 3 s in production and 1 s under `cfg(test)`. It caps discretionary waiting, not the job, and is not a claim about the fence's lifetime.
- **How it travels:** `write_children` passes it to the **local** holder's `holder_write` → `child_write_keyed` → `persist_child` → `persist_locked`. These use it at the scan and in `verify_owned_rows`, for `ChildRole::Holder` only. Every other caller passes none.
- **Shutdown:** a closing authority is a hard refusal, so the next admission ends the wait.
- **Cancellation:** a dropped caller's persist job runs on, but its wait ends at the deadline.
- **Guards:** the persist job keeps its admission read guard and its parent stripe across the wait. The retirement's clearing path needs neither (DC287-HOLDER-FENCE-WAIT-PLAN R2.3); the queued-writer and fence-outliving cases are cut off by the deadline.

### D4. Controls
**The retirement class:**
- a `None`-only mark;
- `Some → None`, then a stale clear (the entry survives);
- an older `Some` ignored, a newer one advancing, and a matching clear;
- snapshot and mixed classes;
- a mark-before-clear handoff while a reader loops (continuous coverage by construction).

**Admission:**
- unresolved-op alone admits on both paths;
- unresolved-op plus `LOG_INDEXED` refuses with `UnresolvedOpIntent` and is not eligible;
- `LOG_INDEXED` alone is eligible; mixed, unindexed and snapshot are not;
- the error text is unchanged.

**The holder read:**
- an eligible wait is witnessed, then cleared → the rows are read;
- the not-eligible cases → the same error at once (the witness stays 0);
- a persistent fence → today's error at the deadline;
- a close during a wait → it ends promptly;
- a ready store's holder reads, at the scan and at verification → **zero wake registrations** (a counter of actual `enable`s);
- a clear between the first, unregistered eligible refusal and the first arm (its wake lost) → the armed recheck admits, one registration and no wait.

**The persist path,** reusing `child_writes` `holder_retirement_fence` (A–D, unchanged with no wait):
- a self-clearing fence after the durable frame, with the append witness armed → **the first attempt acknowledges** with no re-append;
- the same at the scan of a resumed child.

**Through the production loop:**
- a write with several local children under a persistent fence, on an RF3 leader, ends within one deadline, not one per child;
- a **caller deadline shorter than the cap**, through `coordinate`: once the leader's owned holder is witnessed in its eligible wait, the end it was forwarded is the caller's deadline, and its owned work ends by it, not by the cap. A mutant that ignores the caller's deadline must fail this control.

**The public path (RF3, `aged_l2`, run once):** the baseline's ordering, with the candidate's synchronisation:
1. witness the eligible wait;
2. **release the Retire at once**;
3. then verify the holder's completion, the acknowledgement, Finalize and the caller's answer for the same write.

**Lanes:** fmt, clippy against the base, the architecture test, the focused suites, the full gate (with `daemons_worker_stack`) and the **real three-daemon smoke**.

## Consequences
- **What improves:** a holder whose own read meets a logged retirement fence that clears within the attempt's bound now completes its child, with the full existing proof. Its parent can finalize instead of answering `WRITE_STALLED`.
- **What stays as today:** every other refusal, the gate and the final decision.
- **A bounded cost:** a waiting holder can delay a base activation that fences its stripe, by at most the bound.
- **Not addressed:** the append between the reads, the replica role, remote holders, the RF1 Section 8 attribution, and recording refusal text in the write-stall trace.
