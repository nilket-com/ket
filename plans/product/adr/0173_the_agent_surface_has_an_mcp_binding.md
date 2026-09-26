# ADR 0173: the agent surface has an MCP binding

Status: accepted 2026-09-25. Application lane. Decides the protocol-binding
part of what ADR 0158 §8 left open ("an MCP or any protocol binding") and
extends ADR 0166: every operation an agent can perform through Polariton's
command surface is also available as a Model Context Protocol tool, through
a binding that adds no operation of its own. Agent identity and
authorization, the rest of §8, remain undecided.

Agents drive Polariton today through its own wire protocol (PRODUCT-0160): a
per-window, owner-restricted socket (a named pipe on Windows) carrying
newline-delimited JSON commands and events, reached through the `polariton`
command line (`windows`, `act`, `observe`) or the batch driver (`drive`).
The protocol's receipts are what make agent control trustworthy: outcomes
distinguish applied, refused, failed and superseded work; executions, figure
incarnations and connection generations are named; a reply lost in transit
is reported as unknown and never replayed (ADRs 0166, 0168, 0169).

Using it takes work an MCP host would do for the agent. An agent writes a
commands file, runs a command line, filters an event stream for the one
terminal answer, and learns the vocabulary and its arguments from records
and fixtures. MCP hosts (Claude Code, Codex CLI, desktop assistants,
editors) instead discover tools with JSON Schema inputs, call them with
typed arguments and receive structured results. Polariton offers none of
that today: it has no JSON-RPC, no `initialize` handshake, no tool
discovery, and no MCP transport.

This record targets MCP revision 2025-06-18: its lifecycle, tools (with
structured output and output schemas), resources with subscription,
cancellation, and the stdio transport.

## Decision

### 1. A binding over the existing surface, not a second surface

`polariton mcp` is an MCP server on standard input and output, started by
the agent's host as a child process under the person's own user. It owns no
independent application model: live commands go to a window over the same
socket or pipe, through the same client code, that `polariton act` uses, and
batch runs invoke the same driver `polariton drive` runs. The server holds
only what a bridge must: the MCP session, in-flight requests, window
connections and subscriptions.

No operation exists only in MCP, and no MCP tool performs anything the
command surface cannot; ADR 0166 §2's single application operation per
action is unchanged. PRODUCT-0160's wire protocol remains the canonical agent
interface, and the command line and the MCP server are two of its clients.
The command line's verb `mcp` is added beside `windows`, `act`, `observe`,
`serve`, `drive` and `describe`.

### 2. Lifecycle and version

The server follows the 2025-06-18 lifecycle. In `initialize` it answers a
revision it supports with that revision, and any other offered revision
with the one it supports (2025-06-18 in this cut), leaving a client that
cannot use it to disconnect; it does not reject the offer outright. It
declares the `tools` capability and `resources` with `subscribe` and
`listChanged`. During the handshake it handles `initialize` and `ping`;
tool and resource requests are served only after the client's
`notifications/initialized`, and are answered with a protocol error before
it. A second `initialize`, or another lifecycle request out of order, is
answered with a protocol error. Notifications carry no request identity and
are never answered: one that arrives out of order is logged and ignored.
Traffic that makes the session unusable closes it.

### 3. Tools come from the command vocabulary

Each command verb of the live surface (`describe`, `describe_notebook`,
`open_data`, `replace_cell`, `run_cell`, `run_source`, `hide`, `show`,
`isolate`, `restore`, `resize`, `publish`, `connect`, `disconnect`,
`connection_status`) is one MCP tool. Two tools cover what the command line
does beside a window's verbs: `windows`, which lists advertised windows, and
`drive`, which runs a batch (§6).

A tool's input schema is generated, by a schema generator whose output a
test checks, from the same Rust argument type the wire decodes for that
verb, not written by hand. The routing envelope (`window`) is modeled
separately from the command's verb discriminator, so the verb's arguments
are its own schema. Generated schemas are self-contained (no external
references) and are checked for defaults, optional fields and tagged
variants. Every tool and argument carries a description; where confusion is
likely (window identity, a view name as opposed to a figure incarnation,
execution mode, a path relative to what) the description says which and
gives an example.

A window tool takes an optional `window`, the window's advertised identity.
When it is omitted, the server resolves it once, at the start of the call,
and only if exactly one window is advertised; the call is pinned to that
window. If the pinned window disappears the call is answered as §4
describes; it is never retargeted to a window that appeared since. With
several windows the call ends in a bridge error (§4) naming them; with none,
the bridge error says how to open a window that accepts agents (`polariton
--agents`) or to use `drive`.

Tools declare MCP's annotations explicitly: `readOnlyHint`,
`destructiveHint`, `idempotentHint` and `openWorldHint`. The describe tools,
`connection_status` and `windows` are read-only. `run_source`, `run_cell`
and `drive` execute the person's code with its full authority (ADR 0171 §2):
they are not read-only, not idempotent, and open-world, and their
descriptions say so. `replace_cell`, `publish` and `disconnect` are marked
destructive, conservatively: each can replace or end something that exists,
though `publish` keeps its own refusal and witness rules (ADR 0166). Nothing
is marked idempotent that is not. Annotations are hints for hosts, not
authorization; they restrict nothing, and descriptions state a tool's actual
authority and possible effects rather than relying on them.

### 4. Every result is the event

A live tool call returns the exact terminal event the command surface
produces for that command, matched to the call's own request, as the tool's structured content, unchanged:
`completed` or `applied` with `terminal: true` (with its receipt),
`refused` (with its reason and, where the window gave one, a receipt),
`failed` (with its error and receipt), `superseded` (with the figure and its
current incarnation, and no receipt), or `unknown` (with `applied` and a
reason). No field is added and none is invented; a nonterminal `applied` is
not completion, a `changed` event does not settle a call, and the call keeps
waiting past both. The same JSON is also
returned as text content, for hosts that pass text only to the model, with
a short summary before it.

`windows` and `drive` are not live commands and have their own result
objects: `{"windows": [...]}`, the advertisements, and for `drive` the batch
receipt document (§6).

Some calls end without any event from a window or receipt from the driver:
the window is ambiguous or absent, the pinned window disappears before the
command is sent, the driver cannot be started, or it leaves no valid
receipt. These end in a **bridge error**, a result object of its own with a
distinct discriminator (`"bridge_error"`, never an event name), a reason,
and a `dispatch` field saying what is known of the command: `not_sent`, or
`unknown` when it may have been delivered or executed. The binding never
presents a bridge error as a window's `refused`; a window refuses only what
it received. An event received from a window is always returned as it came,
never replaced by a bridge error.

Every tool declares an output schema covering its result and the bridge
error.

Refused, failed, superseded and unknown outcomes and bridge errors set the
result's `isError`, which tells the client the call did not succeed, and
still carry the complete structured result; whether and how a host shows
that to a person or a model is the host's choice. Malformed requests,
unknown tools and arguments that fail the input schema are JSON-RPC errors,
not tool results.

### 5. Lost replies, cancellation and shutdown

There are two boundaries, and neither is retried. If the window's reply to
the MCP server is lost after the command was sent, the tool returns
`unknown` exactly as `polariton act` reports it, with whether an `applied`
was seen first. If the MCP server's reply to the host is lost, the server
cannot report it; the host sees its own transport failure. The binding
never replays a command. A host that calls a tool again makes a new request,
which may repeat the command's effects; mutating tools say so in their
descriptions.

`notifications/cancelled` for a live call, the host's end of input and
server shutdown all abandon the wait: the server stops waiting, releases the
connection and sends no further response for that request. They do not
undo or cancel a command already sent to a window, and there is no
Polariton cancellation verb behind them. A response and a cancellation that
race produce at most one response.

Every phase of a call before its command is sent is bounded: discovery,
connecting and the window's welcome use the command-line client's existing
bounds (a 5 s welcome, 2 s to open a busy named pipe), and starting the
driver is bounded likewise; the implementation records each bound in the
evidence. A bound that expires before sending returns a bridge error with
`dispatch: not_sent`, and one that expires at a point where the command may
have been delivered returns `unknown`; neither resends. The wait for a
command's terminal event, once it is sent, has no bridge deadline, by
decision: how long `run_source` or `publish` may take is the application's
to bound, as it is for `polariton act`, and a bridge deadline would report
`unknown` for work still running. That wait ends when the event arrives,
when the window's connection closes (`unknown`), or when the host cancels.

Tool calls are independent: several may be in flight at once, to the same
window or different ones, and a long call does not block `ping`, discovery
or resource requests.

### 6. `drive` is a child process

The `drive` tool runs `polariton drive` as a child process of the MCP
server, with arguments passed directly (never through a shell), its own
standard input, output and error, and the source and commands the call
names. Each call gives the driver a fresh receipt location of its own, so a
receipt left by an earlier run cannot be read as this call's. Its output never reaches the MCP server's standard output, which
carries only protocol messages. The tool takes the source path, the command
list and the execution mode explicitly (ADR 0169's `on-open` or
`by-command`; the tool has no default), and resolves relative paths against
the server's working directory, which its description states.

The result is the receipt document the driver writes, whenever one is
written and valid, even if the child then exits abnormally; the result's
`isError` follows the commands' outcomes in it, not the child's exit status
or the document's existence, so a batch with a refused or failed command is
an error. If the child leaves no receipt, or one that is missing, truncated
or invalid, the result is a bridge error with `dispatch: unknown`, its exit
status and a bounded tail of its error output: the source may already have
changed files, started processes or reached the network, and the binding
neither infers a rollback nor retries.

Each call owns its child. Cancelling the call, the host's end of input and
server shutdown terminate and reap it. That stops the driver; it does not
undo what the source did, and processes the source itself started may
outlive it.

### 7. Observation is a resource

Each advertised window is a resource, `polariton://window/<id>`, named by
the window's 32-character advertised identity, which a reopened window does
not share. Reading it returns the window's advertisement and its current
figures, with their incarnations and statuses, as `describe` reports them.
It is a view of current state, not an event log: `changed` events produce
`notifications/resources/updated` for subscribed windows, several changes
may produce one notification, and a read returns the latest state.
`resources/unsubscribe` stops them. A window that has closed is not found.

The resource list comes from the same advertisement discovery `polariton
windows` uses, and `notifications/resources/list_changed` is sent when that
list changes. Whether a host shows these notifications to its model is the
host's choice; the `describe` tool remains the way to read a window with
tools alone.

### 8. Transport and trust

Standard input and output only. There is no network listener, no HTTP
transport, and no new authority: the server runs as the person's user,
reaches only the windows that user can already reach, and through `drive`
runs a headless Polariton as that user, with the authority a source has
under ADR 0171 §2. Diagnostics go to standard error.

Whether the server uses the official Rust MCP SDK or a JSON-RPC loop of its
own, and which schema generator, is an implementation choice made with a
dependency and license review under ket's licensing records. An SDK is
preferred over a hand-written protocol layer.

## This cut

`polariton mcp` over standard input and output, with §§1–8: tools for the
live verbs, `windows` and `drive`, with generated input and output schemas
and annotations; the event mapping of §4; the lost-reply and cancellation
behavior of §5; the window resources of §7. Nothing changes in the wire
protocol or the application.

## Acceptance

1. Every live command verb has exactly one tool. Each tool's input schema is
   generated from the verb's argument type and checked: a test fails when a
   verb is added without a tool or a schema changes without its snapshot.
   For every verb, representative valid, invalid and defaulted inputs are
   checked against the generated schema and against the wire's
   deserializer, and the two must agree on which are accepted; each
   accepted input, sent through the binding and directly over the wire,
   must deserialize to equal arguments, omitted and defaulted fields
   included. Output
   schemas validate the results produced in 3–8, bridge errors included.
2. Lifecycle: a client offering 2025-06-18 gets it; a client offering a
   later revision gets 2025-06-18; `ping` is answered during the handshake;
   tool and resource requests before `notifications/initialized`, a second
   `initialize`, and other out-of-order lifecycle requests get protocol
   errors; an out-of-order notification gets no response.
3. Against a live window fixture, `describe`, `run_source`, `resize` and
   `publish` return, as structured content, the terminal event the window
   sent for that same dispatch, captured at the connection, equal as JSON;
   the text content carries the same JSON. Where `polariton act` is
   compared, it runs over deterministic fixtures, since separate executions
   name separate incarnations.
4. A refused command (`run_source` on a notebook), a failed one, a
   superseded one, a nonterminal `applied` followed by `completed`, and a
   connection dropped after `applied` (reported as `unknown` with
   `applied: true`) each map as §4 describes, and no command is sent twice.
5. Concurrency and lifetime: two calls in flight return independently;
   `ping` and `tools/list` answer during a long `run_source`; a cancelled
   call gets no response and its command is not resent; the window closing
   after the command is sent answers `unknown`; an omitted `window` whose
   sole window closes and reopens is not retargeted.
6. Bridge errors: no window, two windows, the pinned window gone before
   sending, and a window that never sends its welcome each return a bridge
   error with `dispatch: not_sent`, within the recorded bounds, and none is
   reported as `refused`.
7. Subscribing to a window and running its source produces an update
   notification and a read that shows the new incarnations; after
   unsubscribing, none; a closed window's resource is not found; opening a
   second window changes the resource list.
8. `drive` runs `uat/treasury_rates.rn` in `by-command` mode and returns
   the driver's receipt document; nothing the child prints reaches the
   protocol stream. A batch whose receipt records a refused command sets
   `isError` though the child exited cleanly. A child killed from outside
   the server mid-run, before writing its receipt, returns a bridge error
   with `dispatch: unknown`; one killed after writing a valid receipt
   returns that receipt. Cancelling a `drive` call gets no response, and
   its child is terminated and reaped.
9. With `polariton mcp` configured as an MCP server in each host, Claude
   Code and Codex CLI each drive `uat/treasury_rates.rn` in a window
   opened beforehand with `polariton --agents uat/treasury_rates.rn`.
   Using tools alone, and no shell commands, the agent finds the window,
   runs the source, discovers the figures and their view names, resizes a
   view by name and publishes. The evidence records each host's version and
   configuration, the tool calls it emitted and the results it received,
   and the published package. A hand-written JSON-RPC harness is not host
   evidence.

## Limits and follow-up

- No HTTP transport, prompts, sampling or authorization.
- Resources carry figure descriptions, not rendered images; a link from a
  resource to a published package's artifacts is a follow-up, not a new
  rendering operation.
- The binding inherits the command surface's platform coverage (Linux,
  Windows named pipes, macOS under ADR 0170) and adds none of its own.
