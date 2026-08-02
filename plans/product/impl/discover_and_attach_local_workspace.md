# Discover and attach the local Toron workspace

Scope: implement PRODUCT-0005. Nilket's runtime identity becomes a public,
versioned, read-only discovery contract; Polariton consumes it, attaches without
ceremony when a workspace verifies, and stops carrying a private copy of the
product's endpoint. No process is started, stopped, cleaned, or owned.
Deliverable is one `impl:` commit reviewed against this plan.

## Crate boundary

Add `nilket/crates/nilket-local`, admitted by the Nilket workspace. It owns the
runtime schema, profile layout, and every validation predicate. The `nilket`
binary depends on it to publish; consumers depend on it to observe.

Move out of `nilket/crates/nilket/src/local.rs`, without behavior change:

| Item | Current location |
|---|---|
| `FORMAT_VERSION` | `local.rs:29` |
| `SQL_ENDPOINT`, `WENDELON_ENDPOINT` | `local.rs:30-31` |
| `RuntimeManifest` | `local.rs:100-110` |
| `ProfileMarker` | `local.rs:94-98` |
| profile path layout | `local.rs:112-141` |
| `resolve_profile_root` / `absolute_lexical` | `local.rs:258-284` |
| `process_start_token` | `local.rs:805` |

The supervisor keeps sole write authority: manifest publication, removal on
exit, lock acquisition, reset, PKI, and configuration generation stay in
`local.rs`. The new crate never writes to a profile.

Dependencies are `serde`, `serde_json`, `toml`, `uuid`, `thiserror`,
`toron-client`, `tokio`, and `libc` for the Linux device major/minor extraction
the lock comparison needs. It does not depend on `nilket`, `toron`, `wendelon`,
`wendelon::cli`, `fs2`, `nix`, or `signal-hook`. `libc` is admitted for
`major`/`minor` decomposition only, never for locking, signalling, or process
control; the architecture guard names the permitted symbols.

## Public API

```rust
pub enum LocalWorkspace {
    Absent { profile_root: PathBuf },
    Starting { profile_root: PathBuf, owner_pid: u32 },
    Ready(ReadyWorkspace),
    Unhealthy { identity: WorkspaceIdentity, cause: String },
    Stale { identity: WorkspaceIdentity, cause: StaleCause },
    Corrupt { context: ObservationContext, artifact: Artifact, cause: String },
    Incompatible {
        context: ObservationContext,
        artifact: VersionedArtifact,
        found: u32,
        supported: u32,
    },
    Unverifiable { context: ObservationContext, evidence: Evidence, cause: String },
}

pub enum Artifact { ProfileRoot, ProfileMarker, RuntimeManifest, OwnershipLock }

/// The artifacts that carry a schema version. Narrower than `Artifact` on
/// purpose: skew is unrepresentable for a root or a lock file.
pub enum VersionedArtifact { ProfileMarker, RuntimeManifest }

pub enum Evidence {
    ProfileRoot,
    ProfileMarker,
    RuntimeManifest,
    OwnershipLock,
    OwnerProcess,
    StableIdentity,
}

/// The strongest identity established before an observation settled. Context
/// advances only after the corresponding artifact has been verified.
pub enum ObservationContext {
    Root { profile_root: PathBuf },
    Profile { profile_root: PathBuf, profile_uuid: Uuid },
    Runtime(WorkspaceIdentity),
}

pub enum StaleCause {
    LockFree,
    OwnerMismatch { lock_owner_pid: u32 },
    OwnerGone,
    PidRecycled,
}

pub struct ReadyWorkspace {
    pub sql_endpoint: SocketAddr,
    pub identity: WorkspaceIdentity,
}

pub struct WorkspaceIdentity {
    pub profile_root: PathBuf,
    pub profile_uuid: Uuid,
    pub session_nonce: Uuid,
    pub owner_pid: u32,
    pub started_unix_ms: u128,
}
```

Two public entry points over one internal classifier:

- `async fn discover(profile_root: Option<&Path>) -> LocalWorkspace` — identity,
  the async `toron-client` health exchange, then re-observation. Polariton's
  entry point.
- `fn blocking_discover(profile_root: Option<&Path>) -> LocalWorkspace` — the
  same composition over `blocking_health_loopback`. The supervisor's entry point.

The identity classifier stays private, because a verified pre-health identity is
not a `LocalWorkspace` and must not be forced into one:

```rust
enum IdentityObservation {
    /// Every filesystem and `/proc` predicate passed; health is the only
    /// remaining question.
    Verified { identity: WorkspaceIdentity, sql_endpoint: SocketAddr, lock_owner_pid: u32 },
    /// A terminal state that needs no health exchange.
    Settled(LocalWorkspace),
}
```

`Settled` can carry any variant except `Ready` and `Unhealthy`, both of which
require health. No public API returns a pre-health value, so no consumer can
attach on identity alone. Publishing a separate pre-health enum is deferred until
something needs it; nothing does today.

The classifier covers steps 1-21 below; the composing entry points add health and
the step 22-25 re-observation, so identity stability is enforced in one place
rather than per consumer.

`WorkspaceIdentity` carries `profile_root`, which consumers legitimately need to
name what they attached to. No accessor exposes the runtime-manifest path, the
manifest struct, a file handle, or parsed JSON. `Display` renderings are the
consumer's diagnostic surface. `ObservationContext` preserves progressive
identity for refusals without hiding typed coordinates inside `cause`: root-only
failures carry `Root`, a decoded marker advances to `Profile`, and a fully
verified manifest advances to `Runtime`.

## Classification

Every artifact is read in three separable steps — **acquire bytes**, **parse the
version header**, **decode the body that version defines** — because each step
fails into a different state. Failure to acquire is `Unverifiable`; failure to
parse is `Corrupt`; a header naming an unimplemented version is `Incompatible`;
failure to decode the body of an implemented version is `Corrupt`. Decoding never
precedes the version check, so a future schema that changes required fields
reports skew rather than damage.

Both versioned artifacts are subject to this: `ProfileMarker.format_version` and
`RuntimeManifest.version` are independent skew axes and both are validated.

**Shapes, then ownership, then contents.** The lock is observed before the marker
is read, because the supervisor acquires the lock at `local.rs:161` and only
creates the marker afterwards in `ensure_profile` (`local.rs:172`). A first-ever
boot therefore presents a held lock and no marker. Any marker-first ordering
reports that as `Absent`, contradicting the guarantee that `Starting` covers the
whole cold boot.

Evaluated in order, first match wins:

1. profile-root inspection finds no root → `Absent`; any other inspection
   failure → `Unverifiable { Root, ProfileRoot }`
2. root is a symlink or not a directory → `Corrupt { Root, ProfileRoot }`
3. marker, manifest, or lock present as a symlink or non-regular file →
   `Corrupt { that artifact }`
4. lock observation fails → `Unverifiable { Root, OwnershipLock }`
5. marker missing:
   - lock held → `Starting { owner_pid }`
   - lock free, root empty or holding only the lock → `Absent`
   - lock free, any other content present → `Corrupt { Root, ProfileRoot }`
6. marker unreadable (permission, I/O) → `Unverifiable { Root, ProfileMarker }`
7. marker version header unparseable → `Corrupt { Root, ProfileMarker }`
8. `marker.format_version != FORMAT_VERSION` →
   `Incompatible { Root, ProfileMarker }`
9. marker body decode fails → `Corrupt { Root, ProfileMarker }`
10. manifest missing → lock held ? `Starting { owner_pid }` : `Absent`
11. manifest unreadable (permission, I/O) →
    `Unverifiable { Profile, RuntimeManifest }`
12. manifest version header unparseable → `Corrupt { Profile, RuntimeManifest }`
13. `manifest.version != FORMAT_VERSION` →
    `Incompatible { Profile, RuntimeManifest }`
14. manifest body decode fails → `Corrupt { Profile, RuntimeManifest }`
15. `manifest.profile_uuid != marker.profile_uuid` →
    `Corrupt { Profile, RuntimeManifest }`
16. lock not held → `Stale { LockFree }`
17. lock holder PID ≠ `manifest.pid` → `Stale { OwnerMismatch }`
18. `/proc/<pid>` unreadable for a reason other than absence →
    `Unverifiable { Runtime, OwnerProcess }`
19. `/proc/<pid>` absent → `Stale { OwnerGone }`
20. recomputed start token ≠ `manifest.process_start_token` → `Stale { PidRecycled }`
21. `manifest.sql_endpoint` unparseable or non-loopback →
    `Corrupt { Runtime, RuntimeManifest }`
22. health exchange resolves, success or failure; re-observe identity
23. identity changed → re-classify from step 1 exactly once; a second change →
    `Unverifiable { Runtime, StableIdentity }`
24. identity stable, health failed → `Unhealthy`
25. identity stable, health succeeded → `Ready`

Steps 2-3 preserve the supervisor's artifact intent and deliberately make
ordinary observation strict. `prepare_root_for_lock` (`local.rs:286`) refuses a
symlinked or non-directory root, the supervisor creates regular identity
artifacts, and reset's `refuse_symlink_tree` (`local.rs:475`) refuses links inside
a marked profile. The ordinary startup path does not yet reject every inner
symlink, so implementation brings supervisor validation into alignment with the
new shared predicate rather than claiming that refusal already exists. Neither
writer nor observer follows a link out of the profile to read identity. Use
`symlink_metadata` and require a regular file.

Step 5's third branch is a real distinction, not a hedge. Any content other than
the regular lock file — whether generated (`config/`, `pki/`, `data/`, `logs/`,
or a runtime manifest) or foreign — with no marker and no owner is a layout the
supervisor refuses, usually a profile whose identity artifact was lost. An empty
directory, or one holding only the lock file a previous supervisor created, is
honest absence.

Step 17 is the restart window: a new supervisor holds the lock while the previous
session's manifest is still on disk, in the interval before `local.rs:176-183`
removes it. The two identities must never be fused.

Steps 22-23 close the same window across the health exchange, which is slow enough
for a restart to complete inside it. Re-observation follows **either** outcome.
Confirming only after success would let a restart during the exchange surface as a
false `Unhealthy` — a healthy new session reported as a sick old one, which is the
same fusion defect wearing the opposite sign. Compare lock holder,
`manifest.pid`, `session_nonce`, and `process_start_token`; on change, re-classify
once; a second change is `Unverifiable { StableIdentity }`, which says the profile
would not hold still long enough to be described, not that any artifact was
unreadable.

## Lock observation

The lock is observed, never acquired. `try_lock_exclusive` races the
supervisor's own acquisition at `local.rs:161`, where `WouldBlock` routes into
`report_existing` and a manifest that does not yet exist becomes a hard startup
error. A consumer polling in the background would intermittently break an
unrelated `nilket` startup.

Observation reads `/proc/locks` and matches an `FLOCK` `WRITE` entry against the
lock file's identity, since `fs2` uses `flock(2)` and `fcntl(F_GETLK)` cannot see
BSD locks. Field 5 is `MAJOR:MINOR:INODE` in hex; compare against `st_ino` and
`st_dev` from `symlink_metadata` of `local.lock`, with `st_dev` decomposed through
`libc::major`/`libc::minor` rather than an open-coded shift, which is wrong on
some device-number layouts. Field 4 supplies the owner PID, used both to report
`Starting` and to prove the lock holder is the process the manifest records.
Confirmed against a live deployment:

```text
51: FLOCK  ADVISORY  WRITE 1578218 fc:01:5772022 0 EOF
```

A missing `/proc/locks`, a permission error reading it, or a line whose shape this
parser does not implement is `Unverifiable { OwnershipLock }` carrying the failure,
never silent freedom and never `Corrupt` — the profile is not damaged by the
observer's inability to see it. Absence of a matching entry in a file that parsed
completely is genuine freedom.

## Supervisor reuse

`report_existing` (`local.rs:498-532`) is rewritten over `blocking_discover` so
one classifier serves both products. Its externally visible behavior is
preserved: `Ready` prints the existing `LOCAL_READY` line and exits success;
every other state keeps an exit-failing diagnostic carrying the strongest
identifying context available to it, which for the states reachable there is
usually session and PID but is the profile root alone for early failures.
`Starting` is now reachable and refuses with a distinct "still starting" message
rather than the current `runtime.json is unavailable` text.

`publish_runtime` and manifest construction (`local.rs:229-242`) switch to the
shared type. `LOCAL_READY` and `LocalReady` are untouched; publication and
discovery remain separate contracts.

## Polariton

Feature-gate the dependency exactly as `toron-client` is gated
(`crates/polariton/Cargo.toml:11-12`, `:54`); a build without the `toron`
feature keeps its current behavior minus the hardcoded default.

- Startup dispatches discovery as an ordinary async command. The first frame
  does not await it. `gui/state.rs` gains `Message::LocalWorkspaceDiscovered`.
- On `Ready`, construct the client from `sql_endpoint`, connect through the
  existing `ToronAdapter` path (`adapters/toron/mod.rs:22-27`), set the stage to
  connected, and report the joined profile and session in the status surface.
  The Connect dialog does not open.
- On every other state, no connection is established and no dialog opens. The
  status surface names the state and its cause.
- `adapters/toron/mod.rs:9-15` loses `default_value` and the literal
  `127.0.0.1:7742`; the placeholder becomes a generic `host:port`. Manual
  connection to any Toron, local or not, is unchanged.
- The stderr-only diagnostic at `adapters/driver.rs:310-320` and the generic
  `"Adapter connection failed."` at `gui/state.rs:920-933` are replaced by the
  typed rendering. The real cause reaches the interface.
- Rune paths return a typed `local workspace unavailable` result carrying the
  discovery state, replacing the fixed strings at `adapters/common.rs:87`,
  `gui/state.rs:557-563`, and `adapters/toron/mod.rs:75`.

Scratch SQL survives as an explicitly labeled capability: an unattached session
still targets the in-memory SQLite database and still records provenance
`"scratch"` (`adapters/driver.rs:101-118`). What ends is the masquerade — a
failed discovery or failed attach must set a state the status surface names, so
a booting deployment can never be silently rendered as a working workspace. Rune
has no scratch equivalent and refuses.

## Tests

1. **Classification table:** a fixture per state and per cause, each asserting the
   exact variant rather than "not ready".
   - `Absent` — no root; empty root with a free lock; marker with no manifest and
     a free lock.
   - `Corrupt` — `ProfileRoot` for a symlinked root, a non-directory root, and
     generated content with no marker and a free lock; `ProfileMarker` and
     `RuntimeManifest` for unparseable headers and for bodies invalid at a
     supported version; `RuntimeManifest` for UUID mismatch and non-loopback
     endpoint; and one artifact-shape case each for a symlinked marker, manifest,
     and lock, asserting the reported `Artifact`.
   - `Incompatible` — `ProfileMarker` with `format_version = 2`, and separately a
     `RuntimeManifest` with `version = 2`. Each asserts the reported
     `VersionedArtifact`. A manifest at version 2 whose *body* is not v1-decodable
     must still report `Incompatible`, proving the version header is read first.
   - `Stale` — `LockFree`; `OwnerMismatch` from a helper holding the lock beside a
     manifest naming a different PID; `OwnerGone` with a reaped PID;
     `PidRecycled` with a mismatched start token.
   - `Starting` — **two** fixtures, both from a helper that acquires `flock` on
     `local.lock` and signals a barrier before the observer runs: one with no
     marker, reproducing first-ever boot before `ensure_profile`, and one with a
     marker but no manifest, reproducing the pre-publication window. The first
     fails under any marker-first ordering and is the regression guard for it.
     These fixtures are the authority for `Starting`; no timing-dependent path
     proves it.
   - `Unhealthy` — verified identity, nothing listening on the endpoint.
   - `Unverifiable` — an injected root-inspection failure; unreadable marker and
     unreadable manifest via injected readers or mode bits; an injected
     lock-observation failure; an unreadable `/proc/<pid>` distinguished from an
     absent one; and `StableIdentity` from the churn case in test 4. Each asserts
     the reported `Evidence` and strongest `ObservationContext`.
2. **Read-only assay:** discovery over every fixture leaves the profile
   byte-identical — same entries, contents, inode, and mtimes — and creates no
   new file. A positive control mutating the profile must fail the assay.
3. **Lock race, deterministically synchronized:** the test must not hope to hit a
   microsecond window, and the observer — not the helper — is the party whose lock
   behavior is under test. A helper that already held the lock would starve the
   competing acquisition under both implementations and prove nothing. Two
   barrier-controlled branches over one acquisition helper:
   - **safe observer** pauses inside its `/proc/locks` observation step holding no
     `flock`; the acquiring helper must succeed;
   - **positive control** substitutes `try_lock_exclusive` and pauses at the same
     barrier while holding the resulting `flock`; the same helper must receive
     `WouldBlock`.

   The branches differ only in the observation strategy, so the assay isolates it.
   The control additionally asserts that a real `nilket local` startup in that
   position dies with the `report_existing` diagnostic, tying the mechanism to the
   product failure it prevents. A secondary soak — a discovery loop against a
   `--data-dir` profile started and stopped repeatedly — is retained as a smoke
   check only, and is explicitly not the proof.
4. **Identity stability, both signs:** with an injected health exchange that
   restarts the profile mid-flight, discovery must re-classify rather than fuse
   two sessions. Two cases, because re-observation follows either outcome: a
   *succeeding* exchange must not yield a `Ready` pairing the pre-health manifest
   with the post-health session, and a *failing* exchange must not yield
   `Unhealthy` blaming the new session for the old one's silence. A second
   substitution yields `Unverifiable { StableIdentity }`, not a third retry.
5. **Attach polarity:** a counter proves no state other than `Ready` constructs a
   client or opens the Connect dialog, and that `Corrupt`, `Incompatible`, and
   `Unverifiable` never reach the previously hardcoded endpoint.
6. **Async render:** startup produces its first frame with discovery still
   outstanding, using a discovery future that never resolves.
7. **Supervisor parity:** `report_existing` over a live profile prints the
   unchanged `LOCAL_READY` line; each failing state exits nonzero carrying the
   strongest identifying context that state possesses through
   `ObservationContext` — session and PID where a manifest verified, profile UUID
   where only the marker did, and the resolved profile root otherwise. `Absent`
   and early `Corrupt` have no recorded identity to name, and the assertion must
   not demand one.
8. **Live workspace:** extend `LiveLocal` in
   `nilket/crates/nilket/tests/cli_integration.rs` under `LIVE_LOCAL_LOCK` and
   the existing `#[ignore]` gate. It proves the two states that only a real
   deployment can produce: `Ready` after `LOCAL_READY`, with a session nonce
   matching the published line, and `Stale` after the supervisor is killed without
   cleanup. It does **not** assert `Starting` — the window is roughly 400 ms and
   unsynchronizable from outside, so a live assertion would be flaky in one
   direction and vacuous in the other. Test 1's flock fixture owns `Starting`. It
   shares the fixed ports, so it stays serialized.
9. **Architecture guards, with synthetic positive controls:** Polariton source
   contains neither `runtime.json`, nor a manifest field name, nor the canonical
   Toron port; `nilket-local` acquires no lock, writes no profile path, and
   spawns no process; `libc` use is confined to `major`/`minor`; the manifest and
   marker schemas have exactly one definition each. Failures cite PRODUCT-0005.
   Extend the existing cross-product assay at `local.rs:967-988` rather than
   adding a second one.

## Non-goals

- No `ket local`, composed session, auto-start, or any answer to `Absent`.
- No manifest cleanup, repair, reset, or signaling from a consumer.
- No runtime schema change; this lands at `version = 1`. `Incompatible` is
  proven with a synthetic fixture.
- No new health definition, readiness contract, or `LOCAL_READY` change.
- No auto-reconnect, polling loop, or retry policy; discovery runs at startup
  and on explicit user action.
- No change to Baryon, libaryon, Wendelon, Toron execution, or the Rune engine.
- No PRODUCT-0002 promotion credit and no change to the default SQL target.
- Linux only, consistent with TORON-532 section 10; `/proc` is a stated
  dependency of the observation strategy.

## Review checklist

- [ ] PRODUCT-0005 accepted; `nilket-local` owns schema, layout, and predicates.
- [ ] Eight variants classified in the specified order, each covered by a fixture
      asserting the exact variant and its reported artifact, evidence, or cause.
- [ ] Both versioned artifacts are version-checked, and every artifact is decoded
      version-header-first so schema skew can never report as damage.
- [ ] Ownership is observed before identity artifacts are read; both `Starting`
      fixtures pass, including the no-marker first-boot case.
- [ ] Observation failure reports `Unverifiable`, never `Corrupt`; symlinked or
      non-regular root, marker, manifest, and lock artifacts refuse.
- [ ] Root I/O failure reports `Evidence::ProfileRoot`; refusal variants preserve
      the strongest verified `ObservationContext` without encoding identity in
      prose.
- [ ] `Artifact` covers root, marker, manifest, and lock; `Incompatible` takes the
      narrower `VersionedArtifact`; `Evidence` includes `StableIdentity`.
- [ ] The identity classifier is private and returns `IdentityObservation`; no
      public API yields a pre-health value.
- [ ] `Ready` requires the lock holder to be the recorded PID and re-confirms
      identity after health resolves either way; a mid-health restart
      re-classifies instead of producing `Ready` or `Unhealthy`.
- [ ] The lock is observed via `/proc/locks` and never acquired; the race assay
      has both barrier-controlled branches, with the observer — never the helper —
      holding the control's `flock`.
- [ ] `Starting` is proven by the controlled flock fixtures, not by live timing.
- [ ] Discovery performs no write, spawn, or signal, proven byte-identically.
- [ ] Polariton renders before discovery resolves and attaches only on `Ready`.
- [ ] The canonical Toron port and runtime schema are absent from Polariton,
      guard-enforced with positive controls.
- [ ] Typed causes reach the interface; Rune refuses with the discovery state.
- [ ] Scratch SQL remains available and labeled; no failure renders as a
      successful workspace connection.
- [ ] `report_existing` and manifest publication run through the shared
      classifier with unchanged `LOCAL_READY` output.
- [ ] Both workspaces validate green; the live deployment is untouched.
