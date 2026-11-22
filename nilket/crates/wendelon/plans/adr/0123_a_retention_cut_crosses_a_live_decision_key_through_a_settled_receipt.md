# ADR 0123 - A retention cut crosses a live decision key through a settled receipt

## Status
Proposed (local), 2026-11-22.
- **Per Codex:** 01a2027b (the R2 review), 01a2028x (GO to draft, with corrections) and 01a202b5 (GO to record this plan locally and run test-only baselines, with corrections on the T4 bound, the sample for T3, and the L2–L4 accounting). Source: KEY-EXPIRY-PLAN-R3.
- **Implementation:** the paired `impl: adr 0123` commit, built in three reviewed slices (the reducer's settled receipt; the D6 fences; the D7 triggers) with their review corrections folded in. As built:
  - `settlement_is_legal` binds a receipt to its key's namespace and counts (an Accepted delete deleted at least one row; a noop none). `validate` also checks the watermark's reconstructible facts (0 before any commit; never ahead of the clock; never below a sampled current certificate's stamp).
  - The lookup is fenced before decode at the PD frame version in both directions; the previous build's own v1 lookup frames are a fixture with provenance.
  - T3 checks the cheap retained-byte mark before walking the covered records, and stops at the least release.
  - `wendelon_decision_generation_targets_proposed_total{reason}` counts proposal ATTEMPTS by the one reason named; commit attribution is by target digest.
  - Per-store certification work is counted (builds, base entries loaded, staged rows, activations, activation rows).
  - Test-only: fixture-local scaled claim cap and pressure thresholds, set on every voter (never serialized).
- **Capacity, Section 8 and the 1 GiB qualification remain held.**
- **Prerequisite, delivered:** the replacement validation race fix (`aa5a159a5`).
- **Base:** origin/main `aa5a159a5`.

## Context
- **ADR 0121 and 0122** hide and reclaim a governed AppendOnly row once a certified generation's floor passes it.
- **A generation's cut stops at every record whose decision key is live** (`generations.rs` `covered`/`blocking_point`), and keys live for E = 3,960,000 ms (about 66 min).
- **The discriminator** (`3de0bbdf4`, accepted 01a20261) shows that a row aged past a 15 s window is certified and reclaimed only about 66 min after issue. The S8 control saw gid 0 for its whole block for the same reason.
- **The key needs only a compact terminal outcome, not the body.** That covers retry, conflict, resolve and Abort (traced in KEY-EXPIRY-DESIGN). The data/base side never reads keys or erased bodies (D4 trace).

## Decision
### Scope
This ADR changes behaviour **only on a store whose bound retention window is above 0.**

On a zero-window store:
- **Behaviour is equivalent to today:**
  - the same coverage, trigger and answers;
  - no `Settled` key ever exists;
  - no key charge is made (D5).
- **It is not byte-identical to today:**
  - the PD WAL, snapshot, frame and marker versions change (D6);
  - `KeyEntry` is larger in memory, because the enum is sized to its largest variant (D5 measures this).

### D1. The settled receipt
```rust
enum RecordKind { Body, Noop, Settled(Settled) }          // KeyEntry.record
enum Settled {
    Accepted { dec: u64, claim: ClaimDigest, commit_hlc: Hlc, effect: AcceptedEffect },
    Refused  { dec: u64, claim: ClaimDigest, cause: RefusalCause, kind: OperationKind },
    Noop     { counts: DeleteCounts },                    // its dec is KeyEntry.reservation
}
enum AcceptedEffect { Write, Delete(DeleteCounts), Kv }
enum OperationKind  { Write, Delete, Kv }
```
**What the type rules out:**
- a Reserved record cannot be settled;
- a settled Noop has no claim and no HLC;
- counts exist only on an Accepted Delete or a Noop;
- a refusal has no counts.

`DeleteCounts` is the existing selection-count type.

**Conversion:** in `commit_generation`'s apply, each covered body or noop whose key is still live becomes `Settled`, built from that record; then the record is erased. A covered record whose key has already retired is erased with no receipt, as today.

### D2. Coverage (positive window only)
- **A cut t covers every record with reservation ≤ t that is terminal with dec ≤ t,** in every namespace (governed, zero-history and unrelated tables, KV, deletes, aborts, CTAS chunks), whether its key is live or not.
- **What still blocks a cut** (`blocking_point`):
  - a Reserved record, or one concluded after t;
  - and, as today, a pending target.
- **At window 0, coverage is today's rule:** a live key blocks.

### D3. Readers
Answers before conversion, after it, and after expiry:

| Request | Answer |
|---|---|
| Write retry | the same terminal answer, before and after conversion |
| Delete retry | the same terminal answer, with exact counts only when Accepted |
| Refusal | no counts |
| Another digest under the same key | `Conflict` |
| Cross-table reuse | `Conflict`: `table_operations` is pruned only with the key |
| Public resolve | `PRESENT` with dec, `commit_hlc` and counts; `Refused`; or `Noop` with no HLC |
| Abort | by origin: Accepted or Refused with a matching claim gets the terminal answer, another claim gets `Conflict`; a Noop gets `Conflict` (DC-284) |
| After expiry | `OutcomeUnavailable`, `Absent{eligible:false}` or `EXPIRED`, as today |
| `lookup_parent` | gains `Lookup::Settled` |

### D4. The base and the receipt are independent
- **Unchanged (traced, R2 section D4):** activation, reads (`active == committed`), reopen, snapshot install, replacement `install_base`, and data-side refusals keyed by reservation. None of them reads keys.
- **Backup hold:** `KvDataPresent` also holds for a **live `Settled` KV key**, alongside the kept-body and base-entry checks.
- **K_min** moves sooner. A reader whose cut is below the new `committed_at` is refused with exactly `cut expired: below K_min`.
- **A released key never authorizes a second application** (`fresh_clock`, with a monotone floor and clock).

### D5. Accounting (positive window only)
- **Admission and the charge:**
  - The reducer refuses Reserve and RecordNoop while the store is **unbound** (the existing typed `FormationIncomplete`: Reserve already refused it; RecordNoop now does too). So every key is admitted under a known binding.
  - At first admission on a positive-window store, `retained_key_bytes` is charged a fixed `KEY_RECORD_CHARGE`, with `checked_add`; over the cap gets `RetainedFull`.
  - At window 0, nothing is charged (the zero-window admission contract is unchanged).
- **`KEY_RECORD_CHARGE`** (175 bytes) is a conservative, representation-level bound: the postcard length of `(ParentKey, KeyEntry)` with every field at its type's maximum, tight over every variant. Not every such encoding is legal (an all-ones uuid is not a UUIDv7; `u32::MAX` counts exceed a 512-row delete; a body receipt needs dec > reservation). The largest legal boundary encoding is 164 bytes. An encoding test asserts both.
- **When it is held and released:**
  - A replay is answered before any charge.
  - Conversion neither charges nor releases.
  - `advance_floor` releases the charge when it retires a key (`checked_sub`).
- **`validate`, both directions, using only facts that a later snapshot can reconstruct:**
  - every key has exactly one record of its kind;
  - a `Settled` key's reservation is ≤ the **current** certificate's t, and no kept body or noop has that reservation;
  - **reservations are unique across all keys** (kept-body reservations, noop decisions, and settled reservations together), so two keys cannot name one settled reservation;
  - a `Settled` body origin has `reservation ≤ dec ≤ current t`;
  - every table key has its `table_operations` entry, and each entry names exactly one key;
  - positive window: `retained_key_bytes == keys.len() × KEY_RECORD_CHARGE`; window 0: `retained_key_bytes == 0`.
- **What witnesses a conversion** once its own certificate is gone: the current certificate exists, the reservation is at or below its t, and the record is absent. The generation that converted it is not recorded, and nothing needs it. **"Above the predecessor's t" is deliberately not an invariant,** because a later snapshot cannot check it.
- **Three measures, kept distinct:**
  - the encoded cap (provisional, proposed at 32 MiB): **an admission bound only**. It is not a resident-memory claim;
  - resident memory: `keys` and `table_operations` B-tree nodes, enum padding, and the **transient whole-base load in `build_candidate`** (per generation);
  - the 1 GiB qualification (held).

### D6. Fences
- **Version bumps (PD only):**
  - WAL and snapshot 35 → 36;
  - frame 28 → 29;
  - marker v25 → v26;
  - the snapshot envelope stays at 1, because it is decoded under snapshot 36.
- **Lookup:**
  - `DECISION_LOOKUP_REQ` and its reply move from the unversioned `HRFT_ENVELOPE_VERSION` to the PD frame version, checked before the body is decoded.
  - `Lookup::Settled` is appended as a new variant index. Old variant indexes are unchanged.
  - **An old peer cannot learn the new check, so the controls show the failure on its side:** the old binary's decode of a new reply fails (an unknown envelope version, or else an unknown variant). It is never read as an old successful lookup.
- **Cut:** unchanged and unversioned. Its contract (`CutAnswer`) carries no record kind.
- **A positive-window store has no upgrade route.** The new binary refuses a v25 store with the existing marker refusal. Before release, such a store has no export or restore route (export `RetentionActive`; restore `RetentionWindowBound`), and the operator recreates it. The binary never recreates anything. The zero-window export and restore route is unchanged.

### D7. Why a target is proposed
The cut t is computed first (D2). It is proposed if any trigger below holds. Each trigger is a pure function of replicated state and the leader's gated clock `now`.

| Trigger | Condition | Windows |
|---|---|---|
| **T0, retired key (today's rule, kept)** | some record ≤ t whose key has retired | all; **the only erasure trigger at window 0** |
| **T1, structure** | a finalized drop or retired CTAS not yet excluded | all |
| **T2, age** | `age_due(now)` | positive |
| **T3, claim pressure** | `now` is present; `retained_claim_bytes ≥ P`; and the covered records hold ≥ R claim bytes | positive |
| **T4, settlement** | `now` is present; `now ≥ settled_ms + S`; and some coverable live-key record has `key.issued_ms ≤ now − S` | positive |

**`settled_ms`** is a new replicated field in `Generations`, monotone.
- At each commit whose certificate carries a sample (`retention.stamp_ms > 0`), `settled_ms = max(settled_ms, stamp_ms)`.
- An unsampled commit (T0 or T1 without a sample) leaves it unchanged, and so does a replaced pending target, which never commits.
- It is in the snapshot, so it survives replay, install and leader change.
- **Genesis:** 0.
- **Sample loss (`now` is `None`):**
  - No T2, T3 or T4.
  - T0 and T1 continue unsampled. They cover and convert as usual and leave `settled_ms` unchanged.
  - Claim pressure is then relieved only by T0 and T1 (today's progress). Admission is refused with `RetainedFull` at the cap, as today.
  - Retries of recorded keys are still answered, because lookup comes before any charge.

**Every T4 target carries the fresh sample** (`retention_cut_for(Some(now))`), even when the age-only floor step is not due.
- The floor it records is `max(current, now − window)`, as on any sampled target. `retention_cut_admissible` already admits that (floor ≤ clock − window).
- A stamp is required for `settled_ms` to advance.

**Policy versus apply:**
- **The triggers and their rates are leader (proposer) policy.**
- **Apply enforces only:**
  - the cut's coverage (D2);
  - `retention_cut_admissible`;
  - the `settled_ms` update rule;
  - the D5 invariants.

  A target does not carry its trigger, so apply cannot and does not enforce a rate.
- **Counting:** when several triggers qualify, the proposer names the target's reason once, in the order T1, T2, T3, T4, T0 (in a counter and a log). Controls count committed generations by that reason.

**Bounds** (none of them is a cleanup deadline):
- **T4 (proposer policy, in sampled-clock time):** **successive T4-triggered commits** are at least S apart.
  - Every sampled commit, T4 included, advances `settled_ms` to its stamp, and the proposer proposes T4 only at `now ≥ settled_ms + S`.
  - This holds across leaders up to the gated clock's bound skew.
- **What this does not bound:** T1, T2 and T3 may commit sampled generations within S of a T4, or of one another.
- **What delays T4:**
  - a blocked cut (a Reserved record, or a pending target), which postpones it;
  - a sampled commit for another reason, which moves `settled_ms`, so the next T4 is due S after that commit;
  - in both cases the delay is reported, never compensated.
- **T4 is never postponed by admission.** Its record condition depends on the oldest uncovered live-key record. That record's age only grows, and the cut is the highest closed t below `applied`.
  - A delayed or old-issue key (`issued` within `fresh_clock`'s bound) is S-old early and is covered by the next eligible T4. That doesn't change the rate.
- **T3:** each **committed** T3 generation releases at least R claim bytes. So committed T3 generations ≤ (the claim bytes retained when this rule first applies + claim bytes admitted since) / R, **initial backlog included.**
  - Proposals that are replaced (a member change) and rebuild attempts (a member's re-prepare after restart) are counted separately. They are bounded by membership changes and member restarts, respectively.
- **No empty work.** Every T3 or T4 target covers a record that is not yet covered.
- **An unresolved parent** caps t, as today. Resolve and takeover remain its path to terminal.
- **T2:** at most one target per `retention_step_ms`, **each a whole-base rebuild.** That is an existing ADR 0121 property, measured here, not changed.
- **Receipt pressure is never a trigger.** `retained_key_bytes` exerts backpressure at admission only.

**Provisional values:** S = 60 s, P = ½ the claim cap, R = ⅛ the claim cap. The controls measure, and claim nothing from the constants.

## Controls (test-only first; RED where the behaviour is new)
**Reducer** (`decisions_tests`, `generations` tests):
- **R1–R6:** a replay across commit; expiry between proposal and commit; expiry after conversion; Reserved at proposal; Abort by origin; snapshot install between conversion and expiry.
- **C1–C3:** the charge constant against the maximal encodings; retries at saturation; a corrupted counter refused.
- **I1:** `validate` refuses a duplicate settled reservation across two keys, a `Settled` above the current t, and a `Settled` coexisting with a kept record.
- **U1:** Reserve and RecordNoop are refused while unbound. A census of in-process tests that admit before binding goes with it.

**In-process cluster** (`TestTime`, three voters):

| Control | What it shows |
|---|---|
| **Z1** | Quiet zero-window expiry: no target until the key retires, then T0 exactly once, and never a `Settled`. |
| **Z2** | Zero-window with a drop: T1 fires and does not cross live keys. |
| **L1** | A 15 s window: row r, and a young survivor y written S − 5 s later. The first T4 at about issue + S has a sampled floor above r: **r is omitted as aged in that same generation**, r's key is `Settled` with unchanged answers, and y survives. The test asserts the actual cut, the receipt, the omission and y. |
| **L1b** | T2 alone: y ages after the first certificate. An age-only target omits y. Then quiescence: no further target while nothing is due. |
| **L2** | Steady load for k·S. T4-triggered commits ≤ k + 1, and commits by reason are reported. **Committed generations are reported separately from rebuild attempts** (`prepare_report` builds per member). **Per-member work** per generation is reported: base entries loaded, staged rows written, activation rows rewritten. Answers stay exact. |
| **L3** | Sustained pressure with an unresolved parent: committed T3 ≤ (backlog + admitted) / R; committed generations, replaced proposals and per-member rebuild attempts each reported separately; admission refused with `RetainedFull`; retries answered; the backlog released once the parent resolves. **Then the clock is withheld:** no T2, T3 or T4; T0 and T1 continue; retries are answered; admission is refused at the cap. |
| **L4** | The exact `settled_ms` after each step of one sequence: genesis (0); an **unsampled structural commit** (T1 with no sample: unchanged); a **pending target replaced** after a member change (unchanged); a **sampled non-T4 commit** (T2: its stamp); a **restart** and a **leader change** (the same value after replay and install); then the **next eligible T4**, proposed no earlier than that value + S, and its own stamp afterwards. No burst of T4. |
| **M1** | An outstanding read cut refuses with `cut expired: below K_min` after an earlier T4 commit. |
| **K1–K3** | `KvDataPresent` with expiry before and after conversion; the zero-window export path unchanged. |

**Wire and binaries:**
- **V1:** the 80c3477cc binary's lookup request and reply bytes (golden, captured from that tree) are refused by the new code before decode.
- **V2:** a new-binary lookup against an old-binary daemon, and the reverse. Each fails and is never read as a successful old lookup.
- **V3:** WAL replay across conversions.
- **V4:** snapshot install with `Settled` keys.
- **V5:** PD profile reopen on the same build, and refusal by the old build.

**Every control** reports what it observed. None claims a physical cleanup deadline.

## Rationale
- **Inline in `KeyEntry`:** one entry per live key, so there is no second map or second invariant. A conversion allocates nothing.
- **Positive-window-only:** a zero-window store has no row to age, and it keeps today's contract.
- **A monotone watermark** bounds T4 against a resettable anchor.

## Consequences
- More frequent generations, **each a whole-base rebuild per member**, measured by L2 and L3.
- K_min moves sooner, so old read cuts are refused earlier.
- `KeyEntry` grows in RAM, including on zero-window stores.
- There is no upgrade route for positive-window stores before release.
- The replacement race fix (now delivered, `aa5a159a5`) was its prerequisite.

## Open Questions
- Values for S, P, R and the cap.
- How much the T2 whole-base rebuild costs at scale.
- Resident memory per key on the 1 GiB node.
