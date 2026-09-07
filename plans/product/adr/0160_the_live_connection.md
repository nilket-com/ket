# ADR 0160: the live connection

Status: accepted 2026-09-07 on the evidence in
`plans/product/impl/0160_the_live_connection_evidence.md`. Application lane,
second record; it consumes
PRODUCT-0158 without changing its verbs or its receipt, and PRODUCT-0156 §6's
posture that actions are the surface and the legend is one client of it.
PRODUCT-0158 gave an agent the application in batch: open a source or a
session headless, run it, act, publish, receive a receipt, exit. Its forward
section named the next step: a person and an agent in one running window,
the agent observing and acting through the same verbs while the person
looks. This record decides how an agent finds and connects to the intended
window, who may connect and what a connection may do, how a command relates
to the person's own edits and to work in flight, what a receipt means when
the application outlives the command, and what bounds the connection. It
decides no new verb.

## Context

### What a running window is

One `AppState`, mutated on one thread by one `update`. Everything
asynchronous - the editor engine, a relation task, a publish - re-enters
that function as a message. A window opened on a saved session carries the
session's id; an ephemeral window carries none. The engine's own wake-up is a
subscription over a channel that yields a unit and lets `update` drain the
engine's queue. Nothing outside the process can send the window a message
today.

### What already exists for a process to be found

The local workspace advertises itself with a manifest under the data root -
its pid, its process start token, its endpoints - and a discoverer decides
liveness from the pid and that token, never from the manifest alone. A
saved session's liveness is the kernel's own lock on its `session.lock`, and
the store records no owning process. `polariton serve` binds loopback only
and carries no authorization at all: the local user is its authority.

### What PRODUCT-0158 left transport-neutral

The verbs, the figure reference by incarnation or view name, the structured
refusals, and the receipt are plain data. The driver's session resolves a
reference against the exports the editor makes, sends the pane message the
legend sends, resizes through the engine's divider drag, and publishes
through the one call the menu makes. All of that runs against the headless
application's `AppState` through the same `update`. A live window has the
same state and the same function; what it lacks is a way for a command to
arrive and a way for a receipt to leave.

## Decision

### 1. A window advertises itself, and an agent names it

Every window opens a Unix domain socket in a per-user runtime directory -
`$XDG_RUNTIME_DIR/polariton/`, falling back to the state root's `run/` -
and writes an advertisement beside it: an opaque **window id** minted at
startup, the socket path, the pid and process start token, the session id
when the window holds one, the source paths its buffers were opened on, and
the start time. `polariton windows` lists the windows whose process is alive
by pid and token, in the shape `--session list` uses, and prunes the rest.
An agent connects to a window by id, by `--session <id>` when exactly one
live window holds that session, or by `--source <path>` when exactly one
live window has it open. Two candidates refuse with both listed; none
refuses with the list of what is live. The advertisement is never trusted
for liveness: the connection itself is the proof.

### 2. The local user is the authority; the person is the gate

The directory is created mode 0700 and the socket mode 0600, and the window
checks the connecting peer's credentials against its own uid: another
user's process cannot connect, and no token is minted or stored. That is the
same trust the local workspace and the served package already extend to the
local user, stated rather than assumed.

What a connection may do is the person's decision, not the socket's. A
window accepts connections only while **accepting agents** is on: the
`--agents` startup flag or the menu item under Plot, off by default, with
the count of connected agents in the status bar while any is connected.
When it is off the socket is not opened. Turning it on starts a new
**connection generation**; turning it off ends the generation, sends
`closed` to every connection on a best-effort basis, and drops them.
Observing and acting need nothing beyond the connection; a per-action
prompt would make the person the agent's mouse again, which is the thing
PRODUCT-0158 ended. There is no agent identity in this cut and no per-verb
permission: the person turns the surface on for whoever they run.

### 3. Every command is a message on the update thread, checked at the gate

A connection is a subscription. The listener task reads one request per
line - a request id and a PRODUCT-0158 `Command` - and yields it into the
application as a message carrying the connection's generation and a reply
handle. `update` first checks the gate: a request whose generation is not
the current one, or that arrives with accepting off, is refused as
`gate_closed` without touching the state. Closing sockets removes nothing
already queued for `update`; this check is what does. Then `update`
resolves the figure reference against the exports at that moment, applies
the command through the same code the headless session uses, and answers on
the reply handle. Nothing runs concurrently with the person's own events: a
command is one `update` call, atomic against the keystroke before it and the
pointer event after it. A rerun replaces the incarnation, so a command
carrying the old token is refused with the current one; a view name resolves
to whatever stands in the pane now. The person's edits are never merged
with an agent's command, because there is nothing to merge: both are
messages in one queue.

An authored view name must identify exactly one live figure in the window.
Two open sources may author the same name; that reference refuses as
`ambiguous_figure` with their incarnation tokens rather than choosing the
first pane. An incarnation reference remains exact. This was grounded during
the action cut, where a running window can retain results from several sources.

The driver's session logic moves from the headless application onto the
application state itself, and the headless application and the live
connection are two callers of it. A guard holds that there is one resolver,
one action path, and one publish call.

### 4. What a receipt means when the application outlives the command

Every request carries an id the client chose: an integer, strictly
increasing within the connection. The window remembers one number per
connection, the highest id it has accepted, and refuses any id at or below
it as `duplicate_request` before anything runs, so the check costs one
comparison and no history. That protection is scoped to the connection and
to nothing else: a new connection starts with no memory of the old one, so
reconnecting provides no deduplication, and there is deliberately no
cross-connection record of outcomes. The window never replays a request on
its own, and a client must not replay a request whose outcome it does not
know.

Answers, each with the request id:

- **applied** - the message ran on the update thread and the receipt's
  `state` is the pane as of that call. `describe`, `hide`, `show`,
  `isolate`, and `restore` are complete when applied: the hidden set is
  resolved in the same call.
- **refused** - nothing ran; the reason is PRODUCT-0158's structured
  refusal, plus `gate_closed`, `duplicate_request`, and `busy` from this
  record. A stale incarnation names the current one.
- **completed** - a second answer for the two verbs whose effect lands
  later, correlated by the request id and by the incarnation the command
  was applied to. `resize` sends the divider drag when applied and completes
  when the engine's projection next carries that pane with a size other than
  the one it had, or completes with the applied answer when the pane already
  had the requested size. `publish` spawns the menu's task when applied and
  completes with the package root, digest, and figures when that task
  returns.
- **superseded** - the completion that cannot arrive: the pane's
  incarnation changed, or the pane left the viewport, between applied and
  the projection. The answer names what replaced it. A person's own resize
  in that window does not supersede an agent's resize; the next projection
  completes it with whatever size the pane then has, and the answer says
  the size is the pane's, not the request's.
- **failed** - the effect ran and did not finish as asked. A publish whose
  write fails after creating files answers `failed` with the publisher's
  reason and the root it wrote into. A resize whose divider drag moved
  nothing - the engine clamped it, or the layout absorbed it - produces no
  projection with a new size and would otherwise never end; it answers
  `failed` when a fixed deadline of five seconds passes without one, with
  the size the pane has at that moment and no claim that anything was
  rolled back, because nothing was. `refused` is reserved for nothing
  having run.
- **changed** - unsolicited, whenever a figure's incarnation, hidden set,
  or size changes for any reason, agent or person: the figures as a
  describe would report them. An agent that acts on a stale picture is
  refused by 3, so this is a convenience, not a correctness requirement.
- **closed** - the window is closing, accepting agents was turned off, or
  the connection is being dropped; delivered on a best-effort basis, since
  the transport may already be gone.

A request with no answer has an **unknown outcome**. The command may have
run before its reply was lost; a later describe can show the pane's state
but cannot say whether this request produced it, whether a publish
happened, or whether a hide was since undone. The client reports the
request as unknown, does not replay it automatically, and observes again.
A command that was applied stays applied whatever became of the
connection: it is the person's application, and it does what it was told.

A batch script over the live connection advances past a `resize` or a
`publish` only when its completion, supersession, or failure has arrived,
so a script's receipt reads the same as PRODUCT-0158's.

### 5. The connection is bounded

Per connection: a request line is at most 64 KiB, and a longer one closes
the connection; at most 16 requests may be outstanding - sent and not yet
answered with a final answer - and a further one is refused `busy` without
being queued; at most 256 answers may wait for a client that has stopped
reading, and when that bound is reached the connection is dropped with a
best-effort `closed`. Answers to requests are never discarded to make room.
`changed` is coalesced to one event per `update` and only the latest
pending snapshot is kept: a slow reader receives the newest picture, not a
backlog of old ones. Nothing a client does can block the update thread: the
reply handle never waits, and a full queue is a disconnect, not a stall.

### 6. Observe first

The first cut is `polariton windows` and `polariton observe <window>`:
connect, receive the figures as loaded and live, then `changed` events until
disconnect, and nothing else. `loaded` on the live connection is each
figure's **load baseline**: the state the pane had when its incarnation
first finished loading - its kernel installed and its first projection
received - captured by the window at that moment and kept for as long as
that incarnation stands. It is not the state at window startup, when the
window may hold no figure yet, and not the state at connection time. A
rerun or a new source installs a new incarnation and captures a new
baseline for it; the old baseline goes with the old incarnation. A client
that joins a window the person has already changed sees that history in
the gap between `loaded` and `live`, and PRODUCT-0158's headless session
captures the same baseline at the same moment. It proves 1, 2, 5, and the transport, and it
proves that observing a window equals describing the same source headless
under the same data, viewport size, startup settings, and hidden state. The
second cut adds the existing verbs - visibility, resize, publish - as
`polariton act <window> --commands <file>` with the answers of 4. No verb
is added in either cut, and none is added until the shared-session
behaviour of 3 and 4 is proven under a person's concurrent use.

### 7. What this record does not decide

Agent identity or per-agent permission; a network transport or a protocol
binding beyond newline-delimited JSON on a Unix socket; a command that edits
a figure's specification, which stays Rune and the inspector's write-back;
an agent opening or closing windows; and any change to what a kernel, a
pane, or the publisher does.

## Acceptance gates

1. **Found and refused correctly.** Two windows on the same source: the
   `--source` form refuses and lists both; the id form connects to the
   named one. A window whose process died leaves an advertisement that
   `windows` prunes and that connecting refuses.
2. **The person is the gate, including for what is already queued.** With
   accepting off, no socket exists; turning it on opens one; turning it off
   with a client connected delivers `closed` and disconnects it; a request
   queued before the gate closed is refused `gate_closed` when `update`
   reaches it, and one queued under a previous generation is refused after
   an off-then-on cycle; the status bar shows the count.
3. **Observe equals describe.** For each semantic showcase open in a window
   at the headless startup size and settings, with no series hidden,
   `observe`'s figures equal `describe`'s on the same source headless, save
   the incarnation tokens and pane ids; after the person hides a series and
   a client joins, `loaded` is still the window's load-time state and
   `live` carries the hidden set.
4. **One update thread.** A guard asserts the connection reaches the state
   only through a message and the shared session logic; the headless
   application and the live connection share the resolver, the action path,
   and the publish call.
5. **Every request ends.** A resize completes with the projected size, or
   at once when satisfied, or is superseded by a rerun between applied and
   projection with the new incarnation named, or fails at the deadline
   with the pane's observed size when the drag moved nothing; a publish
   completes with the package, refuses before running, or fails after with
   the root it wrote; a stale token is refused with the current one; a
   request id at or below the connection's high-water mark is refused
   `duplicate_request`, and the same id on a fresh connection is accepted,
   which the test states as the absence of cross-connection deduplication;
   a request beyond the outstanding bound is refused `busy`; disconnecting
   mid-resize leaves the pane resized.
6. **Concurrent use.** A person's rerun between an agent's describe and its
   hide yields a stale refusal naming the new incarnation, and the hide by
   view name applies to it; a person's own legend click and an agent's hide
   on the same series in either order leave the pane where the legend alone
   would.
7. **Bounded.** A client that stops reading is dropped once its answers
   reach the bound, with the window's update loop never waiting on it, and
   a client that reads slowly receives the latest `changed` snapshot rather
   than a backlog; an oversized request line closes the connection.
8. **No new verb**, and the PRODUCT-0158 receipt tests still pass on the
   live answers.

## Guardrails and stop conditions

1. If a verb needs a code path the GUI does not have, stop, as PRODUCT-0158
   guardrail 1 says.
2. If the live path needs its own copy of resolve, act, or publish, stop:
   move the shared logic, do not duplicate it.
3. If any answer can be delayed by work the person is doing, that is the
   design; if any answer can be lost while the connection is open and
   reading, that is a defect.
4. If a request's outcome can be made known after a lost reply by anything
   short of the client's own record, that is a second source of truth;
   stop rather than add it.
5. Recheck the record number before the plan commit; two commits per cut,
   plan then `impl:`, corrections amended under review, no push until the
   cut is called stable.

## Risks

- **The socket becomes an attack surface.** Bounded by 2: same uid only,
  off by default, and no verb that edits a specification or reaches a
  secret. The publish verb writes a directory the agent names; the
  publisher's own secret guard applies as it does from the menu.
- **A client double-applies after a lost reply.** Not prevented across a
  reconnect, by design: there is no cross-connection record. Bounded by 4
  within a connection, and by the rule that a client never replays an
  unknown outcome. The cost of a cross-connection record would be a second
  source of truth, which guardrail 4 refuses.
- **A slow or hostile client grows memory or stalls the window.** Bounded
  by 5: fixed request size, outstanding and reply bounds, and a disconnect
  rather than a wait.
- **Two windows on one session.** The session store forbids it already.

## Forward

Agent identity and a permission model, when more than one agent or more
than one person shares a window. The command-surface posture across the
application - tables, cells, the editor - with this record and
PRODUCT-0158 as its worked examples.

## Action-cut implementation sequence

The observe cut is implemented in `f01a67f3`; its real-window parity and
validation are recorded in
[`0160_the_live_connection_evidence.md`](../impl/0160_the_live_connection_evidence.md).
The next cut completes the decisions above without adding a verb:

1. Move the resolver and command dispatch into the application surface.
   Preserve incarnation-to-pane attribution independently of load baselines
   so retiring a baseline does not erase a stale token's replacement.
2. Capture a publish job on update and execute its one assemble-and-write
   path from the menu, headless driver, or live client. Preflight refusals
   precede side effects; an execution failure reports the attempted root.
3. Track resize completions on application state. The request's reply handle,
   incarnation, original size, and deadline are the pending work, not an
   outcome history. Disconnecting does not undo the dispatched divider drag.
4. Add `act` with sequential script execution. A lost initial answer is
   unknown; a received applied answer followed by a lost final answer is
   applied with completion unknown. Neither case is replayed.
5. Exercise gates 4–8, repeat the existing batch parity tests, and validate
   both heads and the workspace before the local implementation commit.
