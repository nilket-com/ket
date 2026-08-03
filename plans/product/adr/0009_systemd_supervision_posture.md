# ADR 0009: external service supervision of the local deployment

Status: accepted 2026-08-03. This record sanctions an external service manager
supervising a foreground `nilket local`, and reconciles TORON-532 §3 and
PRODUCT-0006 §7 with the Ubuntu user service that `packaging/ubuntu/` installs.
It changes no code. It preserves, and restates, the prohibition on
self-daemonization.

## Context

TORON-532 §3 says of the local profile: *"V1 is foreground-only: no
daemonization, service installation, or background PID management."* PRODUCT-0006
repeats "no daemonization" among its non-goals and argues in §7 that a foreground
supervisor outliving its parent, with no controlling terminal and no service
manager, is a daemon in everything but name.

`packaging/ubuntu/` now installs a systemd **user** service that starts
`nilket local --data-dir <profile>` at login, optionally at boot with linger. The
result is the instant-on experience the product wanted: PRODUCT-0005 discovery
finds `Ready` at launch, Polariton attaches, and PRODUCT-0006's creation offer
never fires.

Both earlier records are therefore literally contradicted by shipped packaging,
and neither says why the contradiction is acceptable. That gap is what this
record closes. The prohibition those records wrote was aimed at a specific
failure — a process that backgrounds itself and invents its own lifetime
management — and the qualifier "with no service manager" in PRODUCT-0006 §7 was
already pointing at the exception without naming it.

## Decision

### 1. External supervision is sanctioned; self-daemonization is not

A service manager may supervise the local deployment. `nilket local` may not
become a daemon.

The distinction is not stylistic. Under external supervision, `nilket` still runs
in the foreground of a supervisor that owns start, stop, restart, logging, and
ordering, and it still knows nothing about any of that. Under
self-daemonization, `nilket` would fork, detach, invent PID files, and answer
questions about its own lifetime that the profile lock and runtime manifest are
not designed to answer.

Sanctioned:

- an external supervisor starting `nilket local` as a foreground process it owns;
- that supervisor deciding restart policy, boot ordering, and log routing.

Still prohibited, and this record strengthens rather than relaxes it:

- `fork`/`setsid` self-backgrounding in any Nilket role or the local supervisor;
- Nilket-owned PID files, or any lifetime authority beyond the profile lock and
  runtime manifest that PRODUCT-0005 already defines;
- Nilket learning that it is under a service manager, or behaving differently
  when it is.

This partially supersedes TORON-532 §3 on the words "service installation" only.
Its foreground-only rule, its process-group isolation, its drain ordering, and
its parent-death contract all remain in force unchanged.

### 2. The unit must preserve Nilket's own ordering guarantees

A supervising unit is correct only if the guarantees TORON-532 built survive it.
Three properties are required of any sanctioned unit:

- **`Type=exec`**, or an equivalent that supervises a foreground process. A type
  that expects the process to background itself would require the behavior
  section 1 prohibits.
- **`KillSignal=SIGINT`.** The supervisor registers a handler for `SIGINT` only,
  and drains Toron before Wendelon on it. A unit stopping with the default
  `SIGTERM` would kill it under default disposition, the roles would die by their
  own parent-death contract, and the ordered drain would exist on paper and never
  run. This is the same finding PRODUCT-0006 recorded for the product-to-Nilket
  edge, applied at the service-manager edge.
- **A stop timeout above the role drain budget**, since Toron and Wendelon each
  carry a 35-second drain. The installed unit uses `TimeoutStopSec=90s` with
  `KillMode=mixed`, so the ordered drain runs first and the cgroup is cleaned only
  if it overruns.

The unit also selects the profile explicitly with `--data-dir` and restricts
permissions with `UMask=0077`, matching the profile's own `0700` posture.

### 3. Lifecycle ownership is unchanged, because the supervisor is not a product

PRODUCT-0006 §8 grants stop authority only through a live child handle held by
the session that created the deployment. A service-managed deployment is created
by systemd, so no product session holds a handle, so no product session may stop
it.

This is exactly the intended behavior and required no new rule: Polariton
discovers such a deployment through PRODUCT-0005, attaches, and never stops what
it did not start. PRODUCT-0006's creation offer correctly does not appear,
because discovery returns `Ready` rather than `Absent`.

The two paths therefore compose without arbitration. Whichever starts the
deployment owns it, and the other attaches.

### 4. Restart policy must fail loudly rather than flap

A restarting unit interacts with the profile lock. If `nilket local` exits
non-zero while a previous instance is still draining, the replacement observes a
held lock, reports the existing identity, fails health, and exits non-zero again.

`Restart=on-failure` with `RestartSec=5s` and systemd's default start limit of
five starts in ten seconds can never trip that limit, because five restarts five
seconds apart span twenty-five seconds. A persistently failing deployment would
therefore retry indefinitely at a low rate and present as a healthy-looking unit
that is quietly never up.

A sanctioned unit must set an explicit `StartLimitIntervalSec` and
`StartLimitBurst` consistent with its `RestartSec`, so that repeated failure
reaches a terminal `failed` state a user can see. Silent flapping is not an
acceptable failure mode for the component the whole product now attaches to on
startup.

### 5. Linger moves the custody premise, and the warning must follow

Startup prints that `crypto.root_kek_provider=dev-insecure` is a dev/test-only
custody path and not a serious deployment boundary. That sentence was written for
a process a person had just started in a terminal they were watching.

`--enable-linger` starts the deployment at boot with no session and no observer.
On a single-user workstation this is not a new exposure of consequence, but the
premise under the warning has moved, and a later security record must inherit
that fact rather than rediscover it. Installation documentation states plainly
that linger runs a development-custody deployment unattended from boot.

Enabling linger remains an explicit, separately requested step. It is never
implied by installing the service.

### 6. Availability provided by the operating system earns no promotion credit

With the service installed, local Toron availability is lazy and default. That
resembles stage 4 of PRODUCT-0002 §9, and it is not.

§9 gates default status on experienced behavior: bounded startup and memory,
durable restart and upgrade, cancellation and clean shutdown, streaming results,
a green Rune workspace acceptance corpus, honest capability refusal, and repeated
idle and active soaks. Installing a unit changes *who provides* availability, not
whether that evidence exists. The checklist remains open, and a working service
does not retire it.

Likewise, a service-started deployment is an explicit user choice under TORON-532
§9 and earns no Toron promotion credit.

### 7. Scope

Linux and systemd user services, consistent with TORON-532 §10 and the
`io_uring`/`O_DIRECT` substrate. System-wide units, other init systems, and other
platforms require their own evidence and their own record. Packaging never
invokes `sudo`, and an installer that cannot complete without administrator
approval reports what to run rather than escalating.

## Rejected

- **Leaving the contradiction unrecorded:** a reader of TORON-532 §3 would
  believe something the repository disproves, and the next author would either
  remove working packaging or quietly widen an accepted rule.
- **Permitting self-daemonization now that a service exists:** the service is the
  reason self-daemonization is unnecessary, not a precedent for it.
- **Teaching Nilket that it is service-managed:** conditional behavior would make
  the supervised and unsupervised paths diverge, and the divergence would be
  discovered in production.
- **`KillSignal=SIGTERM`:** silently discards the Toron-before-Wendelon drain the
  supervisor implements.
- **A stop timeout below the drain budget:** converts an ordered shutdown into a
  cgroup kill on every stop.
- **Default start limits with a five-second restart delay:** unreachable by
  construction, so persistent failure presents as indefinite quiet flapping.
- **Enabling linger by default:** changes custody posture without the user asking.
- **Treating the service as PRODUCT-0002 §9 stage 4:** confuses who supplies
  availability with whether the engine has earned default status.
- **Product code discovering or managing the unit:** PRODUCT-0005 forbids
  discovery from acquiring lifecycle authority, and a service manager is not an
  exception to it.

## Consequences and sequencing

- The shortest path to a working workspace is now installing a unit once. The
  product's own creation path remains for machines without one.
- TORON-532 §3's "service installation" clause is superseded; everything else in
  §3 stands. PRODUCT-0006's non-goal is narrowed to self-daemonization, which is
  what it was protecting.
- The installed unit must be brought into line with section 4 by adding explicit
  start-limit directives; every other property it already satisfies.
- Installation documentation gains the linger custody statement from section 5.
- PRODUCT-0002 §9's promotion checklist stays open, and a future record deciding
  remembered consent or lazy availability must say whether external supervision
  has made either unnecessary rather than assuming it has.
- A future security record inherits the unattended-custody fact rather than
  discovering it.
