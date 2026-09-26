# ADR 0173 host evidence

Real host sessions on 2026-09-25 (America/Chicago), with the debug executable
SHA-256 `046f10c3e9be932f4d4c4beadd06799f5f383416e181297c43b5f61bf1cca2cf`.
The executable's source is the ADR 0173 implementation in this commit.

- `codex/accepted/`: Codex CLI 0.156.1; actual invocation, config, prompt,
  unmodified JSONL transcript, stderr, exit code, window identity, binary hash,
  and publication manifest/digest. Transcript items are `mcp_tool_call`
  (server `polariton`) and `agent_message`, with no `command_execution` items.
- `claude/accepted/`: Claude Code 2.1.282, claude-opus-5-5; the same provenance
  and raw transcript. `--tools ""` disabled built-ins. The init event advertises
  exactly 17 tools, all `mcp__polariton__*`. `invocation.json` records the exact
  argv/cwd/input from `claude/run-host.sh` with label `capped-claude`.
- Both hosts: windows → run_source → describe → resize by view → publish;
  four figures, successful command outcomes, no automatic retries. A window
  was opened before the sequence and closed afterwards. Bootstrap scripts
  are retained alongside the host directories; they are historical invocation
  records with original absolute paths, not portable test runners.
- `*/attempt1/`: retained unsuccessful initial host attempts. Codex connected
  but saw no windows because XDG_RUNTIME_DIR was not forwarded. Its exact
  failed-attempt prompt was not retained. Claude reached resize but received
  resize_deadline against an already squeezed window and stopped before
  publishing. Later runs used explicit runtime configuration and fresh windows.
- `codex/bootstrap-failed.log`: one later GUI bootstrap failed before the host
  was started because an isolated runtime directory had no Wayland compositor;
  the fixture was restarted explicitly through XWayland.

The raw transcripts/configs were checked for credential-shaped strings and
sensitive key fields before copying. No credentials were found; Claude's
`apiKeySource` field is the literal `none`. Absolute local paths, session IDs,
model/version metadata and token-usage counters are retained as provenance.
No user authentication/config files were copied or modified.

Package manifests retain figure/file hashes; package-summary.json records the
manifest hash and receipt digest. Large Arrow datasets, fonts and rendered
files are not duplicated here, so these directories are evidence, not complete
publishable packages.

Limits: native resize may be clamped by the split layout (900×600 requested,
about 865.6×454 observed), and Claude Code rounds large numeric pane IDs through
JavaScript number representation. Receipts report the actual size; commands
in this sequence use string window/view/incarnation identifiers. Both limits
are documented in the implementation evidence, not hidden by retries.

`validation/` retains build/check/test logs and totals; only trailing whitespace
and blank EOF lines were normalized in these logs. Host transcripts remain
byte-for-byte copies. `drive/` records a prior real native batch integration
run, with its own earlier executable hash; the final build's batch protocol and
cancellation behavior are covered again by the retained tests.
