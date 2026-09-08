# Rune scripting spike: decision evidence

2026-09-08. Recommendation: proceed to design a dedicated scripting environment.
The evidence supports the repository's immediate orchestration needs and a
constrained persistent REPL. It does not establish Python ecosystem parity,
unrestricted incremental Rune semantics, or production readiness.

## What ran

The independent package pins upstream Rune 0.14.1, with no patches or compiler
fork. The Rust host adds seven generic functions: read text, create a new file,
create a directory, canonicalize a path, parse/stringify JSON, and execute an
argument-array subprocess. File creation refuses existing evidence paths. Native
string arguments borrow instead of consuming Rune values; a consuming initial
binding caused a real `Cannot take` failure when the script reused a path.

`journey.rn` completed the same 19 public agent actions as the former Python
harness in a real first-run window. This included the example, opening the
repository's Parquet fixture, SQL edits, new plot incarnations and publication.
The successful first package digest was
`643fd87e8807c6021636f8c59c06d8f59787be9c8c4fe2bea158f98ad032d9af`.
Receipts: `/tmp/polariton-rune-CuZS5U/borrowed-receipts`. No Python, jq, pointer
automation or product-private API was used by the Rune script. The host invokes
the existing Polariton binary; it does not reimplement the agent protocol server.

The final runner, with its two-million-instruction budget, repeated all 19
actions and produced the identical digest. Final receipts are in
`/tmp/polariton-rune-final-Ik310A/receipts`; both test windows were closed.

An unknown-answer fake actuator was invoked once and the script stopped with a
nonzero exit. A script returning `Err("explicit failure")` also exited nonzero.
Those controls caught the distinction between a VM execution error and a Rune
main function successfully returning an Err value: the runner must handle both.

Process assertions cover exit code 7, a 40 ms deadline against a process group
with a sleeping child, a 3 MB output bounded to 2 MiB with a truncation flag, and
SIGINT cancellation while waiting for a process. Arguments are never shell-parsed
by the host. `/bin/sh` is used explicitly only by those process fixtures.

## Persistent state: observations, not assumptions

Each accepted input is parsed using Rune's AST. Function/struct/enum declarations
are staged, and a new unit imports the retained bindings and returns their new
values. Previous statements are never executed again. Values remain Rune values;
they are not serialized between inputs. The interpreter and old VM can be dropped
while a retained closure keeps its needed compiled unit alive.

| Probe | Observed result |
|---|---|
| Retained closure calling `helper() = 10`, then new `helper() = 20` | Closure still calls 10; fresh direct call returns 20 |
| Retained function value | Still calls the old definition |
| Retained closure's captured vector changes | Closure observes the later mutation |
| Old struct instance, same declaration in new unit | Field access, type test and a new-unit method work |
| Same type name with a different struct shape | Old value still passes the type test; accessing the newly added field fails |
| Compile error with a candidate redefinition | Prior declarations and bindings remain usable |
| Runtime failure after vector mutation and scalar shadowing | Vector mutation survives; candidate scalar and new binding are not published |
| Runtime failure after creating a file | File remains; a later input does not replay or undo the write |
| Later evaluations after one effectful append | Effect count stays one; no transcript replay |
| Tuple/vector/object destructuring and shadowing | Bindings persist and resolve to the expected values |
| Infinite loop | Two-million-instruction budget halts it; previous state is still usable |
| Reset | Old binding names are unavailable |

The prototype therefore refuses a changed struct/enum declaration until reset.
Function redefinition has snapshot semantics for retained function values and
closures. Those are usable semantics, but they need to be explained rather than
advertised as equivalent to Python's name resolution.

The AST work mattered. In this version, a struct item's derived span omits its
closing delimiter. The adapter uses parsed statement starts and the enclosing
block's closing token to preserve complete source fragments. A line classifier
would also mishandle the multiline and destructuring examples. Rust-style
or-pattern match arms did not compile in the pinned Rune version; the script uses
separate arms. None of these required changing the compiler.

A local debug-host probe compiled and executed 100 successive inputs in about
21 ms with a small retained environment and optimized dependencies. This is only
a smoke measurement on this machine, not a benchmark against Python or a claim
about large sessions. The debug executable was approximately 9.3 MiB on disk.

## What this does not settle

- The adapter supports only a subset of declaration forms. It has not solved
  modules, imports, macros, arbitrary pattern-name resolution, impl replacement,
  source maps, or full-language incremental compilation. Identifiers used by the
  wrapper are reserved. Its handling of a failed input is not transactional.
- Closures retain old units intentionally. Retained-memory accounting, cycles,
  reclamation and long-lived-session growth were not measured. Resetting names
  is not a demonstrated guarantee that every allocation is reclaimed.
- Process handling is Unix-specific and proven for ordinary process groups.
  A descendant that deliberately escapes its group can retain a pipe and defeat
  the current reader join. Native file operations can block independently of VM
  instructions. Windows process trees, binary I/O, stdin streaming, environment,
  working directories and production cancellation need proper implementations.
- The prompt has no completion, history, automatic multiline detection or rich
  diagnostics. Output formatting supports JSON-compatible values, not every Rune
  value. An instruction budget is not a general sandbox or a memory limit.
- Existing Python scripts also need HTTP, TOML, dates, regular expressions and
  binary subprocess streaming. Those are inventoried, not migrated or supplied
  by this seven-function host. No third-party Python package appeared in the four
  older tracked scripts, but replacing their standard-library behavior still
  requires implementation and acceptance tests.

## Project implication

The compiler-fork question is answered narrowly: this runner and this explicit
REPL policy work as downstream code. That is enough to justify a public-project
design, not enough to promise an unrestricted Python replacement in a few days.

The public core should own the host interfaces, execution semantics, diagnostics
and interactive session. Product-specific integration should consume that core
or use the public agent protocol. The proof used generic process/JSON functions,
so it does not require Toron, an editor, a window or an account merely to run.
Reuse of upstream CLI/documentation/language-server facilities remains a design
choice; this prototype exercises the compiler/VM directly.

The next planning cut should specify a supported first release: file execution,
short evaluation, the stated REPL subset, process/filesystem/JSON APIs, stable
errors, installation and a three-platform ladder. It should treat retained-memory
behavior and process-tree cancellation as acceptance work, not polish. Naming,
license/provenance review and the public repository are still undecided.
