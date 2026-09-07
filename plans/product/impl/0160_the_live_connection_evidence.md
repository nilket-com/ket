# PRODUCT-0160 evidence: the live connection

Status: observe cut implemented locally, 2026-09-07. The action cut and its
completion gates remain outstanding. This is not acceptance of the record.

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
from before an off/on cycle. This cut accepts describe only; other existing
verbs explicitly refuse `observe_only` until the action cut shares their
state-owned implementation.

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
application state. The action cut will extend the guard to the shared
resolver, actions, and publisher.

Validation so far: default binary suite 1,238 passed, 25 intentionally ignored;
127 architecture guards passed; reduced-head all-target check passed;
workspace all-target check passed; reduced-head binary suite 911 passed,
12 intentionally ignored; opt-in GPU evidence target compile check passed.
The action cut's validation remains outstanding.

## Remaining action cut

Move resolution, visibility, resize admission, and publish preparation into
the shared application surface; add `act` with correlated applied/final
receipts; implement resize completion, supersession, and the five-second
deadline; retain applied effects across disconnects and report missing final
answers honestly. Prove gates 4–8, including concurrent human actions, publish
failure roots, and the unchanged PRODUCT-0158 receipt tests.
