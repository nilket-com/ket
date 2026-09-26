# ADR 0173 implementation evidence

Status: accepted 2026-09-25.
Plan commit `b69b4564` precedes this implementation. Claude reviewed the
implementation and fixes; both real host acceptance sequences passed.

## Result and boundaries

`polariton mcp` is a stdio binding of the existing command surface. Live calls
select a window once, validate its welcome identity, dispatch once, and return
the same request's terminal event. Nonterminal applied and changed events do
not settle a call. A lost reply remains unknown with its observed applied bit;
no retry or retargeting exists. `polariton act` keeps its prior timing behavior;
the additional connection/write bounds apply only to MCP.

The official Rust SDK rmcp 3.4.1 owns typed JSON-RPC messages, request IDs,
concurrent dispatch and cancellation. The direct-service entry permits an
explicit initialize/initialized gate; the normal SDK server entry permits
requests before initialized. Only protocol 2025-06-18 is supported, including
fallback negotiation from a newer offer. Ping remains responsive during a run.
Notifications receive no responses. The SDK removes cancelled request IDs and
drops their eventual responses; dropping a live call closes its socket without
undoing effects. EOF/shutdown cancels owned work and suppresses output.

The small stdio adapter reports malformed JSON instead of the SDK default's
silent ignore behavior, and stops at EOF instead of draining pending replies.
Input is bounded at 8 MiB including the newline, also across cancellation of a
partial read. Oversized frames close the session without dispatch. A final
unterminated line at EOF is also discarded without dispatch. Explicit non-object
tool arguments are rejected before SDK normalization can turn null into omission.

`driver::Command` is the sole command vocabulary. Schemars derives schemas from
its serde types; the binding separates the window envelope from the external
verb discriminator. The checked-in input snapshot and parity tests compare
schema acceptance, authoritative wire deserialization and decoded arguments,
including defaults. Output schemas derive from the wire types, restrict live
results to terminal events plus unknown, and include the distinct bridge_error
shape. Embedded roots omit $schema/title; terminal applied is constrained true.
The same event/receipt JSON is returned in structured and text content.

Standard JSON Schema accepts an integer-valued number such as 1.0 while the
existing serde u32/u64 decoder rejects that floating representation. The wire
remains authoritative; explicit regressions demonstrate this schema limit and
the resulting JSON-RPC invalid-params error. Numeric bounds otherwise include
u32/u64 maxima, rather than treating format annotations as validation. No claim
of universal lexical schema/decoder equivalence is made.

Window resources contain current advertisements and figures, not an event log.
Typed SDK notifications report subscribed updates and discovery list changes.
Subscriptions are removed on exit without removing a replacement subscription.
An omitted window is resolved once; ambiguous/absent/gone windows are bridge
errors, never fabricated application refusals.

All non-read-only tools conservatively advertise destructiveHint=true, including
hide/show/isolate/restore/resize, which alter existing presentation state. This
extends the ADR's named replace_cell/publish/disconnect examples. Hints are not
authorization. Local Rune retains ADR 0171's full user/process authority.

Each drive call creates a private temporary directory and fresh receipt path,
then invokes the driver executable directly, without a shell, in its own child.
The retained supervisor owns the child through late startup and reaping; only
acknowledgements/results cross channels. Child stdout is isolated, stderr has a
bounded tail, and a valid receipt wins over exit status. Batch outcomes determine
isError even with clean exit. Missing/truncated receipts report unknown, never
rollback. Cancellation/EOF terminate and reap the driver; descendants may outlive it.

## Bounds and validation

- Discovery, MCP connection, welcome, and MCP write: 5 seconds each. The existing
  Windows busy-pipe retry remains 2 seconds. A partial-write regression delivers
  a prefix before timing out and verifies one unknown result with no resend.
- Driver file preparation and startup: 5 seconds each. Preparation timeout is
  not_sent; uncertain startup is unknown, with retained cleanup ownership.
- Driver stderr tail: at most 8192 raw bytes; drain after exit gets 100 ms before
  its handle is closed, so an inheriting descendant cannot hold the call open.
- No bridge deadline after a live send. The batch driver retains its 600-second
  run deadline; host timeouts are separate.
- A timeout cannot interrupt a blocking native filesystem/spawn operation. It
  bounds the caller's wait while cleanup ownership remains retained.

Eighteen focused MCP tests pass, plus the live client's partial-write test.
They cover handshake/version/lifecycle errors, schema parity and defaults,
malformed/oversized framing, exact same-dispatch terminal equality, error and
unknown mappings, nonterminal waiting, concurrency, cancellation without resend,
window ambiguity/close/reopen, subscribed reads and wire notification JSON,
receipt precedence, external kills before/after a receipt, and child reaping.
Notable regressions are `framing_accepts_the_limit_and_closes_before_oversized_input_is_dispatched`,
`integral_float_spelling_is_refused_by_wire_decoder_as_invalid_params`, and
`adr_0173_partial_write_timeout_is_unknown_and_not_replayed`.

`cargo test --locked -p polariton -- --test-threads=1`: **1543 passed, 0 failed,
26 ignored** (1399 application tests, 129 architecture tests, 15 other integration
and compile-fail tests; nested helper-process summaries excluded).
`cargo check --locked -p polariton --no-default-features --tests` and
`cargo build --locked -p polariton` both pass without warnings. Logs and per-binary
totals are retained in `exhibits/0173/validation/`. The previous review iteration
also passed its full suite (1541/0/26). The initial parallel suite
exposed an outdated local-source classification expectation (TIOBE had already
landed alongside Treasury); that test now includes both exhibits. It also hit
`reporting::spool::tests::unpublished_progress_temporary_does_not_skip_a_report`
with a busy drainer lock. No reporting implementation changed; serial full runs
pass. This isolated parallel failure is disclosed, not claimed to have been
reproduced on baseline. Architecture/render-evidence censuses include the new
module and serialization boundary.

## Actual host evidence

Durable artifacts are under [exhibits/0173](exhibits/0173/README.md): versions,
effective configuration, invocation, prompts, unmodified JSONL transcripts,
stderr/exit codes, executable hashes, and package manifests/receipt digests.
The initial unsuccessful attempts are retained too. Credential-shaped strings
and sensitive key fields were checked before copying; no credentials were found.
No user authentication files or repository .codex/.claude configuration were added.

Both final runs used debug executable SHA-256
`046f10c3e9be932f4d4c4beadd06799f5f383416e181297c43b5f61bf1cca2cf`.
A native window was opened with `polariton --agents uat/treasury_rates.rn` before
each sequence; thereafter only MCP tools performed the acceptance actions.
Both sequences were windows → run_source → describe → resize by view → publish,
with explicit window identity, four fresh named views and four published figures.
Both publication receipts report digest
`204595c34a86b00f33e2f1834c59f504f81cd040181659a8bce70322c94ba05c`.

- Codex CLI 0.156.1: window `2973537cf86c40d086159991e50bcb7f`, exit 0. Transcript
  items are mcp_tool_call (server polariton) and agent_message; no command_execution
  items occurred. The initial attempt saw no windows because the host filtered
  XDG_RUNTIME_DIR; the successful invocation forwards the fixture runtime explicitly.
  One later fixture bootstrap failed before the host started because an isolated
  runtime lacked a Wayland compositor; explicitly using XWayland resolved it.
- Claude Code 2.1.282, claude-opus-5-5: window `5c4d3427b3a348d39c0e72ae72c056fb`,
  exit 0. The init event exposes exactly 17 MCP tools and no built-ins
  (`--tools ""`). Every call is pinned to that window, with zero tool errors.
  Its initial attempt received resize_deadline in an already squeezed window
  and stopped before publish. A fresh fixture completed the sequence.

Host/application limits are preserved, not hidden by retries:

- The native split layout clamps a requested 900×600 resize to about 865.6×454
  and can squeeze neighboring panes. Receipts report the actual size. Whether
  applied should require the exact requested size is an application/ADR 0166
  follow-up, outside this binding.
- Claude Code displays large numeric pane IDs (around 2^63) as
  9223372036854776000 after JavaScript-number parsing of structuredContent;
  Codex CLI preserves them exactly. No command input uses pane, and this
  sequence uses string window/view/incarnation identifiers; execution counters
  are small. This host precision limit is a follow-up, not a claim that the
  displayed Claude receipt is bit-exact to the wire.

A prior real MCP drive of Treasury also completed run_source/describe/publish
with four figures and clean EOF (`exhibits/0173/drive/`, including its earlier
executable hash). Final-build regression tests independently cover the child lifecycle.
Large published Arrow/font/render files are not duplicated in the exhibits;
the retained manifests carry their hashes. Historical local paths are provenance,
not a promise that the full packages remain at those paths on another machine.

## Dependencies and license review

Verified against downloaded crate manifests and the lockfile on 2026-09-25:
rmcp 3.4.1 is Apache-2.0 only, with server/stdio features; schemars 1.2.2 and
jsonschema 0.58.0 are MIT. Jsonschema has default features disabled and no HTTP
or filesystem reference retrieval. It is retained as the independent advertised-
schema validator before dispatch and in parity tests: schemars emits schemas,
while serde alone cannot establish what a host schema validator accepts.
Existing tokio-util and tempfile become direct dependencies for tracked
cancellation and private per-call files. No existing package version changes.

| Crate | Version | Manifest license |
| --- | --- | --- |
| `borrow-or-share` | 0.2.4 | MIT-0 |
| `bytecount` | 0.6.9 | Apache-2.0/MIT |
| `email_address` | 0.2.9 | MIT |
| `fancy-regex` | 0.19.2 | MIT |
| `fluent-uri` | 0.4.1 | MIT |
| `fraction` | 0.17.0 | MIT OR Apache-2.0 |
| `jsonschema` | 0.58.0 | MIT |
| `jsonschema-regex` | 0.58.0 | MIT |
| `jsonschema-value` | 0.58.0 | MIT |
| `micromap` | 0.3.0 | MIT |
| `num-cmp` | 0.1.0 | MIT/Apache-2.0 |
| `referencing` | 0.58.0 | MIT |
| `rmcp` | 3.4.1 | Apache-2.0 |
| `schemars` | 1.2.2 | MIT |
| `schemars_derive` | 1.2.2 | MIT |
| `serde_derive_internals` | 0.30.0 | MIT OR Apache-2.0 |
| `unicode-general-category` | 1.1.0 | Apache-2.0 |
| `uuid-simd` | 0.8.0 | MIT |

The implementation adds exactly these 18 locked packages. A full licensing
census refresh was tried during review, then deferred because the September 8
snapshot also accumulated unrelated workspace drift. Updating all census files,
Licensing 0001's historical counts, and the generator's trailing-whitespace
handling belongs in a separate licensing change; none is included here.
