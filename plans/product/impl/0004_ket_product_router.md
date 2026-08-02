# Implement the `ket` product router

Scope: implement PRODUCT-0004 as a new, Linux-only, dependency-free product
binary. `ket` selects one typed route, resolves one leaf executable, and
process-replaces itself. It owns no editor, database, workspace, health, or
lifecycle semantics. Deliverable is one `impl:` commit reviewed against this
plan.

## Product grammar

The implementation exposes exactly:

```text
ket                                  # configured default; factory Baryon
ket [DEFAULT-ARGS...]                # arguments forwarded to that default
ket baryon [ARGS...]                 # exec baryon
ket polariton [ARGS...]              # exec polariton
ket nilket [ARGS...]                 # exec nilket
ket toron [ARGS...]                  # exec nilket toron
ket wendelon [ARGS...]               # exec nilket wendelon
ket default                          # inspect effective default
ket default baryon|polariton|nilket  # set override
ket default reset                    # remove override
ket --help | --version               # router surfaces
```

The first token alone selects router grammar. Unknown first tokens and flags
are default-target arguments, except the router-owned `--help` and `--version`.
Users disambiguate a colliding path or target flag through an explicit route.
Parsing retains arguments as `OsString`; no user argument passes through UTF-8
conversion or a shell.

One authoritative route table supplies parsing, leaf executable, fixed prefix
arguments, help text, and default eligibility:

| Route | Leaf | Fixed prefix | Default-eligible |
|---|---|---|---|
| Baryon | `baryon` | — | yes |
| Polariton | `polariton` | — | yes |
| Nilket | `nilket` | — | yes |
| Toron | `nilket` | `toron` | no |
| Wendelon | `nilket` | `wendelon` | no |

`DefaultTarget` remains a separate closed enum (`Baryon | Polariton | Nilket`),
not a route plus runtime validation. There is no arbitrary executable or
command-string variant.

## Crate and dependency boundary

Add `crates/ket`, admitted automatically by the root `crates/*` workspace. It
builds the `ket` binary and has no `[dependencies]`, `[dev-dependencies]`, or
build dependencies. Production code uses only `std`, including
`std::os::unix::process::CommandExt::exec`.

Non-Linux builds compile only an honest refusing entrypoint so workspace
metadata and source inspection remain possible; they do not emulate true exec
with spawn-and-wait. Linux is the only claimed and tested V1 runtime.

An architecture test reads the manifest and route sources. With synthetic
positive controls it proves:

- no dependency table can acquire a product, analytics, connector, or helper
  crate unnoticed;
- there is one route table and one default-eligibility definition; and
- Toron/Wendelon routes name `nilket` with fixed role prefixes rather than
  standalone role executables.

The guard failure cites PRODUCT-0004 and directs a future breaker to make a new
record before widening the router.

## Visible default artifact

The artifact lives at:

```text
${XDG_CONFIG_HOME:-$HOME/.config}/ket/router.conf
```

It uses one deliberately tiny, strict, dependency-free format:

```text
version=1
default=baryon
```

The default value is exactly `baryon`, `polariton`, or `nilket`. An unsupported
version is a typed version-skew refusal, never absence. Absence means factory
Baryon. An empty `XDG_CONFIG_HOME` is treated as unset. If neither a non-empty
`XDG_CONFIG_HOME` nor `HOME` is available, launching with no override still
uses factory Baryon; inspection or mutation reports that no persistent config
path can be resolved.

Reads are strict: wrong line count, missing final newline, whitespace,
non-UTF-8 bytes, unknown/duplicate keys, unsupported version, unknown target,
non-regular file, or symlink is an actionable refusal and never a fallback.
`ket default` prints the effective target, `factory` or `override`, and the
resolved artifact path without launching anything.

Setting creates the `ket` directory if needed, writes a uniquely named
create-new temporary file in that directory, flushes it, and atomically
renames it over the artifact. It never follows a destination symlink. Reset
removes only a regular, non-symlink artifact that parses successfully; absent
is an idempotent success, while malformed or unrecognized state refuses for
manual inspection. The file contains no secret and needs no private-mode
claim.

## Resolution and exec

Resolve the route's leaf without repository or Cargo knowledge:

1. obtain `current_exe`, derive its sibling path, and inspect it with
   symlink-following metadata;
2. if the sibling is absent, walk `PATH` in order using platform path-splitting
   semantics;
3. accept only executable regular files; compare device/inode identity against
   the running router to prevent a direct or symlinked self-cycle; and
4. build one `Command` with the fixed role prefix followed by the original
   `OsString` arguments, inherit cwd/environment/stdio, then call `exec`.

A present but invalid sibling refuses instead of falling through to `PATH`.
Invalid PATH entries are skipped according to ordinary executable-search
behavior; a self-cycle is an explicit refusal. Resolution errors name the
logical route, leaf executable, inspected sibling, PATH outcome, and an install
hint. An `exec` error names the selected path and OS error, then exits nonzero.
No alternate route or implicit Nilket-local fallback exists.

The install documentation gives source-checkout commands for the router and
its independently installed targets. Toron and Wendelon require only the
`nilket` binary because they are role routes through it. Documentation does
not imply that installing `ket` installs every product binary.

## Tests

1. **Grammar table:** every explicit route produces the exact leaf, fixed
   prefix, and forwarded arguments; help is generated from the same table.
   Unknown first arguments reach the factory/configured default unchanged.
2. **Eligibility:** all three allowed defaults round-trip. Toron and Wendelon
   refuse as defaults while positive controls prove their explicit routes.
3. **Artifact:** absence, set, inspect, replace, and idempotent reset; malformed,
   whitespace, missing-newline, unknown-target, symlink, non-file, and
   unresolved-home refusals. A failed write leaves the prior valid artifact
   readable.
4. **Resolution:** sibling wins over PATH; absent sibling reaches PATH; an
   invalid present sibling refuses; missing PATH target refuses; direct and
   symlinked self-cycles refuse.
5. **Process identity:** an integration helper records its PID, cwd,
   environment, raw argv bytes, stdout/stderr, and exit behavior. The observed
   PID is the launched `ket` PID, proving process replacement rather than a
   waiting wrapper. Non-UTF-8 argv survives byte-identically.
6. **Role ownership:** `ket toron ...` and `ket wendelon ...` reach a fake
   Nilket leaf with exactly one correct prefix. Bare role routes never become
   `nilket` with no role noun and never acquire local-profile arguments.
7. **Corruption polarity:** every invalid explicit config and every broken
   coherent sibling prevents target execution, proven by a launch counter;
   these guards cannot pass merely because execution tests are blind.
8. **Architecture assays:** each negative guard has a synthetic violating
   manifest/source positive control and cites PRODUCT-0004 in its failure.

All process fixtures are Linux and deterministic: no wall-clock sleeps, real
shell parsing, repository target-directory guesses, or installed user binaries.
The tests copy/hard-link controlled helpers into isolated temporary bin/config
directories and restore nothing outside those directories.

## Non-goals

- No `ket local`, composed application/database session, Polariton auto-start,
  attach/health probe, or ownership of a child lifecycle.
- No change to Nilket's zero-argument local policy or role parser.
- No installation bundle, service manager, daemon, plugin system, arbitrary
  command alias, shell completion, or remote routing.
- No changes to Baryon, Polariton, libaryon, Toron, or Wendelon behavior.
- No macOS/Windows runtime claim in V1; support requires preserving the
  process-semantics contract and its own evidence.

## Review checklist

- [ ] PRODUCT-0004 is accepted with the Linux-v1 honesty clause.
- [ ] One route table drives parsing, help, resolution inputs, and fixed role
      prefixes; `DefaultTarget` is the three-variant closed subset.
- [ ] `ket toron`/`wendelon` exec Nilket's role grammar; no standalone Toron
      route or Nilket-local inference.
- [ ] Visible atomic default artifact; corruption refuses; factory Baryon only
      when no explicit artifact exists.
- [ ] Sibling-first resolution, honest PATH fallback, invalid-sibling and
      self-cycle refusals.
- [ ] Linux process replacement proven by PID and byte-exact argv fixtures;
      no shell or retained router process.
- [ ] Zero product/runtime dependencies, guard-enforced with positive controls.
- [ ] Source-install documentation names the independent target requirements.
- [ ] Full root-workspace validation green; nilket workspace and live
      deployment untouched.
