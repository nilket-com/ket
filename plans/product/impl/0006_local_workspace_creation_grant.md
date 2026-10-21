# Implement the local workspace creation grant

Scope: implement PRODUCT-0006. `Absent` gains a visible offer; explicit
acceptance spends one grant to launch Nilket's existing local profile; the
resulting workspace is bound to the initiating session and proved ready through
PRODUCT-0005 discovery. Nilket's startup order changes once. The discovery
observer contract is untouched. Deliverable is one `impl:` commit.

## Two hazards this plan is shaped around

Both were found while mapping the seam and both silently produce the opposite of
the intended behavior.

**A. `PR_SET_PDEATHSIG` is per-thread, and Polariton is multithreaded.** The
parent-death signal fires when the *thread* that forked dies, not when the parent
process exits. TORON-532 is safe because its supervisor spawns from a
main that runs on one thread. Polariton dispatches async work through
`iced::Task::perform` on tokio workers, and CPU work through
`tokio::task::spawn_blocking`, whose pool threads are reaped after an idle
timeout. Spawning the child from either would arm a contract that fires while
Polariton is alive and healthy, killing the user's workspace mid-session.

The child must therefore be spawned from a dedicated `std::thread` created for
this purpose and parked for the remainder of the session, owning the child handle.
That thread's death is the session's death, which is the semantic the record
requires.

**B. The role edge uses `SIGTERM`; this edge must not.** `OwnedChild::spawn`
(`nilket/src/local.rs:595`) arms `set_pdeathsig(Some(Signal::SIGTERM))` for roles,
and the supervisor registers a handler for `SIGINT` only
(`signal_hook::flag::register(SIGINT, ...)`). A product-to-Nilket edge armed with
`SIGTERM` would kill the supervisor under its default disposition: no drain, no
ordering, and the roles then die by their own `SIGTERM` contract with Toron and
Wendelon terminating together. The ordering guarantee would exist on paper and
never run.

Arm this edge with `SIGINT`, which the supervisor already handles and which
drives the existing ordered drain. This reuses TORON-532 rather than amending it.

## Nilket changes

Two changes in `nilket/crates/nilket/src/local.rs`, both correct independently of
the product seam.

1. **Reorder startup.** Move `preflight_endpoints()` (`:126`) ahead of
   `ensure_profile` (`:123`), `ensure_artifacts` (`:124`), and
   `validate_artifacts` (`:125`), yielding: validate candidate, acquire ownership
   lock, typed preflight, generate profile and PKI, start roles, publish runtime.
   A failed attempt may leave the profile root and lock file; it must leave no
   marker and no PKI.
2. **Type the conflict.** `preflight_endpoints` (`:449`) currently returns bare
   strings that name neither the role nor the transport. Return a typed conflict
   carrying role (Wendelon or Toron), transport (UDP or TCP), endpoint, and the
   bind error, rendered by one `Display` impl. Each entry in the two bind lists
   gains its role and transport at the table rather than at the call site, so a
   future endpoint cannot be added without classifying it.

   The conflict must not name or guess the occupant. The two admissible claims
   are distinguished by which observation failed: a held profile lock is another
   Nilket supervisor and is reported by the existing `report_existing` path; an
   occupied endpoint under a freely acquired lock is a creation conflict and
   nothing more.

`--reset` semantics, lock handling, drain order, role contracts, and the
`LOCAL_READY` line are unchanged.

## The creation seam in Polariton

Gated behind the existing `toron` feature. Nothing below is reachable without it.

**Offer.** The discovery handler at `gui/state.rs:952` already stores the
observation in `adapter_state.local_workspace` (`adapters/driver.rs:76`). Only
`LocalWorkspace::Absent` renders an offer naming the profile root that would be
created. Every other state renders its existing diagnostic and no offer, including
`Corrupt`, `Incompatible`, and `Unverifiable`.

**Grant.** Acceptance is a new explicit message. `AppState` holds a one-shot grant
that is consumed on dispatch, not on success. While an attempt is in flight the
offer is replaced by attributed progress; after it settles, no new offer appears
without a fresh user action. There is no retry, backoff, or timer.

**Spawn.** A dedicated `std::thread` is created for the attempt and lives for the
session. It resolves the `nilket` executable, spawns `nilket local`, arms the
parent-death contract, and parks holding the handle. It does not interpret role
output or parse readiness.

Command construction mirrors `OwnedChild::spawn` (`local.rs:578-619`) exactly
where the semantics must match, with one deliberate difference:

- `process_group(0)`, so a terminal's `SIGINT` cannot fan out to the workspace
  behind the initiator's back;
- `pre_exec` arming `set_pdeathsig(Some(Signal::SIGINT))` — **not** `SIGTERM`, per
  hazard B — followed by the captured-parent recheck that closes the
  fork/configure race; and
- stdout and stderr captured and attributed into the existing diagnostic surface
  rather than inherited.

**Watch.** Readiness comes from PRODUCT-0005 and nowhere else. After spawn the
session polls `nilket_local::discover` on a bounded interval until `Ready`, or a
terminal state, or a deadline. Success is never inferred from the spawn
succeeding, from an exit status, or from parsing `LOCAL_READY`. Polling is safe by
construction: PRODUCT-0005's observer acquires no lock, and
`observing_proc_locks_never_blocks_the_supervisor_acquisition` already proves a
concurrent observation cannot starve this very startup.

On `Ready` the existing attach path runs unchanged. On a terminal state or the
deadline, the attempt fails visibly with that state named, and the grant stays
spent.

**Executable resolution.** Resolve `nilket` as the sibling of the running
executable, then via `PATH`, mirroring PRODUCT-0004 section 4 for the same
package-coherence reason. Polariton must not depend on the `ket` router crate. If
a third consumer of this rule ever appears, extract it rather than copy it again;
record that in the failure message so the next author sees the constraint.

## Lifetime and signals

**Graceful exit.** `Message::CloseWindow` (`gui/state.rs:851`) is the only
production quit path, reached from the titlebar (`gui/components.rs:133`) and from
the editor's `:q` (`state.rs:873-875`). When a live handle exists, that arm first
returns a task that sends `SIGINT` to the supervisor and waits for it to exit,
then closes the window.

This deliberately adds close latency, which `AppState::drop` (`:81`) currently
avoids by detaching. The record requires waiting for the ordered drain, so the
wait is bounded by a deadline slightly above Nilket's own drain timeouts, and the
window shows an attributed "stopping local workspace" state rather than appearing
hung. On deadline expiry the session stops waiting and exits; the parent-death
contract then applies.

**Abrupt death.** Polariton installs no signal handlers and has none today, so a
`SIGINT` or `SIGTERM` to the editor bypasses `AppState::drop` entirely. That is
acceptable and is the intended path: process death kills the owning thread, which
fires `pdeathsig(SIGINT)` on the supervisor, which runs the same ordered drain.
Adding signal handling to Polariton is explicitly out of scope; the kernel
contract is the mechanism.

**Attached workspaces.** The stop path is reachable only through a live child
handle. `AdapterState::local_workspace` is an observation and confers nothing. A
session that attached to a workspace it did not start holds no handle and
therefore has no code path that can signal it.

## Tests

1. **Offer authority:** every non-`Absent` variant renders no offer and exposes no
   accept action; `Absent` renders one naming the resolved root. A positive
   control proves the offer can appear, so the negatives cannot pass by the offer
   being unreachable.
2. **Grant is one-shot:** acceptance consumes the grant; a failed attempt leaves
   no offer, schedules no retry, and arms no timer. A counter proves exactly one
   spawn per acceptance.
3. **Thread ownership, hazard A:** the child survives tokio worker churn and
   blocking-pool idle reaping. Spawn the workspace, drive enough async and
   `spawn_blocking` work to retire the threads that serviced the request, idle
   past the blocking pool's timeout, and assert the child is alive. A positive
   control that spawns from a `spawn_blocking` thread must observe the child die,
   proving the test detects the hazard rather than assuming its absence.
4. **Signal choice, hazard B:** a fixture supervisor recording its received signal
   proves the armed parent-death signal is `SIGINT`. A `SIGTERM` control
   demonstrates the drain does not run under default disposition, tying the choice
   to the ordering guarantee it protects.
5. **Parent-death edge:** killing the initiator with `SIGKILL` leaves no surviving
   supervisor or role process. The captured-parent recheck is exercised by the
   same fixture pattern TORON-532 uses.
6. **Graceful drain order:** on `CloseWindow` with a live handle, the supervisor
   receives `SIGINT` and the session waits; assert Toron's exit precedes
   Wendelon's using the supervisor's attributed output. Assert the deadline path
   exits rather than hanging.
7. **Ownership is a handle:** with `local_workspace` reporting a `Ready`
   observation and no live handle, no code path signals anything on exit. Fabricate
   an observation whose identity matches a running deployment and prove it is still
   not stopped — the inference this record forbids.
8. **Readiness by discovery:** a spawn that starts a process which never reaches
   `Ready` fails visibly at the deadline and attaches nothing. A control proves the
   success path attaches only after discovery reports `Ready`.
9. **Preflight ordering and residue:** with 7742 occupied, a fresh profile root
   fails preflight and afterwards contains no marker and no PKI; PRODUCT-0005
   classifies the residue as `Absent`. A positive control proves an unoccupied run
   does generate both.
10. **Typed conflict:** each preflight endpoint reports its role, transport, and
    endpoint. An assay proves both bind tables carry role and transport for every
    entry, with a synthetic untagged entry as the control, so a new endpoint cannot
    be added unclassified. Assert the conflict text names no occupant.
11. **Architecture guards:** Polariton gains no dependency on `ket`, `nilket`,
    `toron`, or `wendelon`; the spawn path exists only under the `toron` feature;
    `nilket-local` still contains no `Command::new`, no signalling, and no lock
    acquisition, so PRODUCT-0005's guards keep passing unchanged.

Process fixtures are Linux, deterministic, and use controlled helper binaries
rather than the installed product or the user's live profile.

## Non-goals

- No remembered consent, mode preference, lazy start, retry, or daemonization.
- No `ket local` route and no change to the PRODUCT-0004 router.
- No signal handling added to Polariton.
- No change to PRODUCT-0005's states, ordering, classification, or observer
  prohibitions.
- No change to Nilket's role parsing, drain timeouts, tenancy, ports, PKI, reset
  semantics, or `LOCAL_READY`.
- No offer on any state other than `Absent`, and no repair of a damaged profile.
- No macOS or Windows claim; the parent-death contract is Linux, as TORON-532
  section 10 already establishes for the profile itself.

## Review checklist

- [ ] PRODUCT-0006 accepted; the offer appears only on `Absent` and only proposes.
- [ ] The grant is one-shot, consumed on dispatch, with no retry or timer.
- [ ] The child is owned by a dedicated session-lifetime thread; hazard A has a
      positive control that observes the failure it prevents.
- [ ] The parent-death signal is `SIGINT`, proven by a receiving fixture, with the
      `SIGTERM` control showing why.
- [ ] `process_group(0)` and the captured-parent recheck are present.
- [ ] Graceful close signals, waits, shows attributed progress, and exits on a
      bounded deadline.
- [ ] Stop authority flows only from a live handle; a matching observation with no
      handle stops nothing.
- [ ] Readiness comes from `nilket_local::discover`, never from spawn success or a
      parsed readiness line.
- [ ] Preflight precedes generation; a failed attempt leaves no marker or PKI and
      classifies as `Absent`.
- [ ] Typed conflicts carry role, transport, and endpoint, and name no occupant.
- [ ] PRODUCT-0005 guards and tests still pass unchanged; both workspaces green;
      the user's live profile untouched.
