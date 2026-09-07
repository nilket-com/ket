# PRODUCT-0160 evidence: the live connection

Status: both implementation cuts are complete and validated locally,
2026-09-07. The ADR remains proposed for acceptance; nothing is pushed.

## Observation

`polariton --agents <source.rn>` advertises its window and opens its socket.
`polariton windows` lists the window id, saved session id when present,
acceptance state, and current source paths. `polariton observe <window-id>`,
`observe --session <id>`, and `observe --source <path>` select the connection.
The latter two refuse ambiguity with the candidate advertisements.

The initial `welcome` carries the actual window identity, generation, and
figures. Subsequent `changed` events carry the newest figures. Observation
uses the same state-owned load baseline as the batch driver. A baseline is
captured after update first exposes a kernel with a projected plot area;
offscreen figures retain it, and replacement incarnations capture a new one.

The Plot menu toggles accepting agents. With it off there is no socket;
with it on, the status bar shows the connected count. Every inbound request
passes the gate and generation check on update, including queued requests
from before an off/on cycle. The action cut now routes every existing verb through the same state-owned
resolver and dispatch used by the headless driver.

The source paths come from the engine's tab projection, including inactive
tabs, rather than from tab titles or just the startup argument. The binary's
opt-in GPU harness mirrors the additional module, as its architecture guard
requires.

## Real-window parity, gate 3

A debug binary opened each showcase in its own ephemeral X11 window on the
local display, against the existing local Nilket workspace. Each window used
the same startup configuration and scale as headless describe. The probe
identified only the child process's X window by `_NET_WM_PID` and delivered
the Run shortcut directly to it; it did not move the pointer or change focus
to another application. Each test window was terminated after its receipts
were captured. No saved session was opened or checkpointed.

For each source, a socket observer received the welcome and loading changes,
then requested describe after every expected view had loaded. Independently,
`polariton describe <source.rn>` produced the batch receipt. The comparison
removed only the top-level `figure` incarnation and `pane` id from each
description; everything else, including loaded/live state, dimensions,
series tables, inspector provenance, and renderer mode, compared equal.

| Source | Figures | Full description equality | Local receipt directory |
|---|---:|---|---|
| `uat/plot_trends_showcase.rn` | 4 | Pass | `/tmp/p16-1yf7b5y1` |
| `uat/plot_composition_showcase.rn` | 3 | Pass | `/tmp/p16-3l49zxb5` |
| `uat/plot_distribution_showcase.rn` | 4 | Pass | `/tmp/p16-k3e6k294` |
| `uat/plot_multivariate_showcase.rn` | 3 | Pass | `/tmp/p16-xjrf287l` |
| `uat/plot_fixtures_showcase.rn` | 3 | Pass | `/tmp/p16-lykflv66` |

Each directory contains `observe.ndjson`, `latest.json`, `headless.json`,
and the window log. These are local run artifacts, not checked-in fixtures.
A separate real-window CLI pass in `/tmp/p16-yyxrtoym` exercised `windows`
and `observe --source`; its final CLI snapshot equalled the direct socket
describe receipt from the same window.
The million-point scatter reported `downsampled` at 942.4 × 432; the trends
scatter reported `vector`. Every loaded hidden set was empty.

## Regression tests

Seven `product_0160` units cover discovery ambiguity and PID reuse, private
directory/socket modes, explicit CLI selection, queued gate generations,
the load baseline before a late client and after rerun, bounded lines and
queues, and real socket admission. The socket test admits sixteen requests,
refuses the seventeenth as busy without enqueuing it, refuses a reused id on
that connection, accepts the same id on a fresh connection, and receives
closed when the server's generation ends.

The architecture guard keeps transport free of application/editor types,
routes it through `Message::AgentInbound`, and holds baseline capture in
application state. The action cut extends the guard to the shared
resolver, actions, and publisher.

Observe-cut validation: default binary suite 1,238 passed, 25 intentionally ignored;
127 architecture guards passed; reduced-head all-target check passed;
workspace all-target check passed; reduced-head binary suite 911 passed,
12 intentionally ignored; opt-in GPU evidence target compile check passed.
The completed action-cut ladder and full gate audit appear below.

## Live actions

`polariton act <window-id> --commands <file>` accepts the same commands file
as the batch driver. `--source` and `--session` selectors also work. The CLI
parses the whole file and enforces the wire line limit before connecting,
then assigns increasing request ids and waits for each final answer.

Synchronous verbs return terminal `applied` receipts. Resize and publish
return nonterminal `applied`, followed by `completed`, `superseded`, or
`failed`; an already-satisfied resize is terminal immediately. Publish's
applied receipt includes the captured figure descriptions to correlate the
package with its source incarnations. Failure events carry their reason
outside the applied receipt; a publish failure also carries the structured
publisher error and attempted root. No failure claims rollback.

The client reports `unknown` with `applied: false` when it loses an initial
answer, and `applied: true` when it received admission but lost completion.
It stops the script without reconnecting or replaying. These client reports
are not server acknowledgements and create no outcome record in the window.

The application owns one resolver and action dispatcher, one resize
completion check, and one captured publish job. The menu, batch, and live
publish callers execute the same job. The architecture guard checks those
call sites and forbids separate resolver/publisher implementations in clients.

Two implementation details surfaced during this cut:

- Retiring a load baseline must not discard a stale token's pane attribution.
  The state retains incarnation-to-pane identities separately from baselines.
- Distinct open sources can author the same view name. Such a reference now
  refuses `ambiguous_figure`, listing the candidate incarnation tokens.
  It never selects the first pane arbitrarily.

## Real-window action exercise

The debug binary drove the seven checked-in fixture commands against a real
ephemeral window. Only the publish directory was changed to a fresh test
root. Receipts and the package are in `/tmp/p16-_3kfbtfj`.

The script received seven final answers in command order. Resize completed
before the following hide and describe; the million-point scatter reported
`rasterized` afterward, and each pane retained its empty load baseline while
live hidden sets became `[0]`, `[1]`, and `[2]`. Publish completed with the
package root and digest. The package contains the corresponding hidden sets.

A targeted Run shortcut then reran the source in that window. A second live
script first used the old hostile-label incarnation and received
`stale_figure` with its replacement, then hid by authored view name and
received the replacement token and hidden set `[1]`. Those receipts are in
`stale.ndjson`. The test window was closed afterward.

## Action-cut regressions

Seventeen `product_0160` tests now cover the observe and action cuts together.
The added controls include byte-identical live/batch figure files and equal
package digests, all synchronous verbs' receipt parity, human/agent hide
ordering on the legend's message path, supersession after a rerun, a resize
surviving disconnect, deadline failure with the observed size, and ambiguous
view names. Client duplex tests verify that scripts wait for final answers
and stop without replay after either form of unknown outcome.

The partial-write test uses an overlong authored figure name. The publisher
writes the dataset before its figure directory fails, and the live completion
handler reports `failed` with that root while leaving the data in place.
The empty-application publish test refuses before a job or directory exists.

The real-socket backpressure test starts a reply larger than the send buffer
and stops reading. Filling the 256-answer queue drops the connection without
waiting on the client. A separate slow-reader control receives only the
newest of 1,000 pending figure snapshots. The oversized-line check also runs
through an actual socket connection.

## Final validation and gate audit

The final debug binary repeated the real-window action and rerun exercise in
`/tmp/p16-seiencvf`. It then requested a 100,000 × 100,000 resize, which the
engine clamped to the viewport, waited for that completion, and repeated the
same request. The second request returned `failed`, reason
`resize_deadline`, with the same observed pane size. Its receipts are
`clamp-1.ndjson` and `clamp-2.ndjson`; this exercises the real timer and the
engine's clamping, in addition to the controlled unit deadline test.

| Gate | Evidence |
|---|---|
| 1: discovery and refusal | Unique-id, ambiguous source/session selection, start-token pruning units; real CLI window listing and source selection |
| 2: the person is the gate | Private directory/socket modes; peer-UID check at accept; socket close and queued old-generation tests; status count and shutdown gate tests |
| 3: observe equals describe | All 17 figures across five real-window showcases; late-client baseline and rerun baseline tests |
| 4: one update thread | Transport has no application authority; architecture guard holds shared resolver, action dispatch, and captured publish job; live/menu/batch publish parity |
| 5: terminal outcomes | Real resize and publish completions; satisfied resize, supersession, and deadline units; real clamping deadline; preflight refusal and partial-write failure; socket duplicate/busy/reconnect tests; disconnect preserves resize |
| 6: concurrent use | Human/agent hide ordering on the legend message path; real Run followed by stale-token refusal and successful view-name action |
| 7: bounds | Actual socket oversized-line closure, blocked-writer queue overflow without waiting, and slow-reader newest-snapshot control |
| 8: existing verbs and receipts | Shared command enum; synchronous receipt parity and byte-identical publish figures; existing PRODUCT-0158 suite in the full run |

Final ladder:

- Default binary units: 1,248 passed, 25 intentionally ignored.
- Reduced-head binary units: 915 passed, 12 intentionally ignored.
- Architecture guards: 127 passed.
- Workspace all-target check: passed.
- Opt-in GPU evidence target compile check: passed.
- Formatting and diff whitespace checks: passed.

The ignored suites retain their existing opt-in requirements; the real-window
runs above explicitly exercised the local workspace and million-point fixture.
The earlier observe-cut measurements remain applicable: the action cut adds
no renderer or baseline implementation of its own.

## Follow-up: truthful batch outcomes

The batch receipt reported a resize that reached its deadline, a resize that
was superseded, and a publish whose write failed as `refused`, which §4
defines as nothing having run. The receipt now carries a third outcome,
`failed`, with a structured `failure` - `resize_deadline`,
`resize_superseded` naming the replacement, or `publish` naming the reason
and the root it wrote into - and keeps the observed state. The live answers
carry the same outcome on their receipts. Applied and refused receipts
serialize exactly as before, which a test asserts key by key; the batch
deadline and write-failure paths each have a test. The second follow-up, a
non-panicking answer when a baseline is missing, is still open.
