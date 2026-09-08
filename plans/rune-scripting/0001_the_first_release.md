# Rune scripting 0001: the first release

Status: proposed 2026-09-08. The first record of a project that does not
yet have a name or a repository: a scripting environment for Rune - a
runner for `.rn` files, a short-expression evaluator, and an interactive
session - built on upstream Rune with no compiler fork. It lives under
`plans/rune-scripting/` in this repository so that it lifts out whole when
the public project is created. This record decides the technical scope of
a first release and what that release must prove. It leaves the project's
name, its license, and its provenance review open, with the boundary of
those decisions stated.

## Context

### Why

The operator's rule for this repository is Rust, Rune, or Bash, and no
Python for durable tooling. The rule has cost: a nineteen-action acceptance
harness was written in Python because nothing else was convenient, and it
had to be rewritten. Bash with `jq` is tolerated and works, but it is
fragile under quoting and shells, and it is not the language the products
speak. Rune is the products' authored representation and is already hosted
by Baryon, Polariton, and Toron. What is missing is a way to run Rune from
a shell against files, JSON, and processes, and to explore it interactively.

### What the spike established

`plans/spikes/rune-scripting/`, 2026-09-08, on upstream Rune 0.14.1 with no
patch: the same nineteen `polariton act` actions run from a `.rn` file with
seven generic host functions and produce the package digest the Bash
harness produces. A persistent session works without a compiler fork
through an adapter over Rune's own AST: function, struct, and enum
declarations are retained and recompiled into each new unit; statements are
compiled into a unit that takes the current bindings and returns the new
ones and run exactly once; bindings survive shadowing and destructuring. A
retained closure or function value keeps calling the definition it was
created against, while a fresh call resolves the new one. An old struct
instance still passes the runtime's type check after its declaration
changes shape, and only an access to a newly added field fails; the
prototype therefore refuses a changed type until reset, because the runtime
does not detect the mismatch itself. A failed input is not transactional:
shared handles mutated before the failure stay mutated, an external write
before a runtime error stands, and nothing replays. A two-million
instruction budget halts a loop and leaves the session usable. One hundred
successive inputs compiled and ran in about 21 ms.

What it did not establish: modules, imports, macros, or `impl` replacement
in a session; retained-memory growth, since closures pin their units;
process handling on anything but Unix process groups; completion, history,
or rich diagnostics; and any of the facilities the repository's four older
Python scripts use beyond files, JSON, and processes.

### What upstream provides

Rune ships a command-line interface - run, check, test, format, doc,
benches, a language server - and exposes it for reuse with custom host
modules. It ships no interactive session. This project builds on that
interface where it fits and owns the session, the host library, and the
diagnostics it needs.

## Decision

### 1. Three entry points, one runtime

`run <file.rn> [args]` executes a file's `main` with its arguments;
`eval <expression>` compiles and runs one input and prints its value; and
`repl` opens an interactive session. All three share one host library and
one set of diagnostics, and a script that runs under `run` runs unchanged
as a sequence of session inputs, because the session's rules are a strict
superset of a file's.

### 2. The session's semantics are stated, not discovered

- **Supported inputs.** Statements, `let` bindings with shadowing and
  tuple, vector, and object destructuring, and function, struct, and enum
  declarations at the top level. Modules, imports, macros, and `impl`
  blocks are refused in a session in this release with a message that
  names the restriction; they work in files.
- **Snapshot semantics for functions.** A function may be redefined. A
  value created before the redefinition - a closure over it, or the
  function as a value - keeps the definition it was created against; a
  call by name after the redefinition resolves the new one. This is a rule
  of the environment and is documented as such; it is not Python's name
  resolution and is not presented as it.
- **Changed types require a reset.** A struct or enum whose shape changes
  is refused until `:reset`, because an existing instance would pass the
  runtime's type check and fail on the new field. Redeclaring an identical
  shape is accepted.
- **A failed input is not transactional.** A compile error changes nothing.
  A runtime error leaves every mutation that happened before it, in memory
  and outside the process, and publishes no new bindings. Nothing is ever
  replayed to reconstruct state. The environment says this on first use of
  the session and in its documentation; a copy-on-write binding model that
  could isolate the in-memory half is a separate contract for a later
  record, never an implicit promise.
- **Reserved names.** The adapter's own identifiers are reserved and
  refused.

### 3. Retained memory has a limit and a measured reset

A session accounts for the units and values it retains. It reports its
retained size on request, refuses to retain past a configured bound with a
message that names `:reset`, and `:reset` is measured: the release's
evidence shows retained memory after reset returning to the empty session's
baseline for the cases the session supports, or states exactly which cases
do not reclaim and why. A retained-memory number is acceptance work in this
release, not polish.

### 4. Processes are cancelled on three platforms

The host runs a child from an argument array, never through a shell, with a
deadline, a bounded capture that reports truncation, and cancellation on
interrupt. On Linux and macOS the child is a process group and cancellation
kills the group; on Windows it is a job object and cancellation terminates
the job. A descendant that escapes its group or job is named as the
remaining gap, with the reader join bounded so it cannot hang the runner.
The three-platform ladder is a release gate, not a follow-on.

### 5. The host library is small, generic, and documented

Arguments, environment, working directory, reading and writing files with
a refuse-to-overwrite form, directory creation and listing, path
canonicalization, JSON parse and stringify, the process call above,
monotonic time and deadlines, and structured errors that carry a code and a
message. Nothing product-specific: no Toron, no editor, no window. A
product wanting more writes a host module against the same interface or
uses its own public protocol, as the journey script does through
`polariton act`. HTTP, TOML, dates, and regular expressions are named as
the second release's candidates after the inventory of the older scripts
says which are needed.

### 6. Diagnostics, installation, and compatibility

Compile and runtime diagnostics use Rune's own with source positions, in a
file, an expression, or a session input. The runner installs as one binary
with `cargo install`, pins an exact upstream Rune version, and carries a
compatibility note per release naming that version and what changed. No
fork of the compiler or the VM; a need that cannot be met downstream is a
stop condition for this record and an upstream conversation.

### 7. Name, license, and provenance are separate decisions

The project has no name yet. Its license is open and is **a decision
separate from the analytics products' licensing**: choosing an open-source
license for a standalone Rune runner settles nothing about Toron's license
or the discovery page's terms, and the reverse is also true. What the
decisions may share is one provenance review of the code that moves,
which is required before any public repository exists and is not
established by commit authorship alone. Until those are settled the code
stays in this repository under `plans/rune-scripting/`.

### 8. What this record does not decide

Python ecosystem parity; a sandbox or a memory limit as security
boundaries, which the instruction budget is not; completion and history
beyond a line editor; a language server integration; and the migration of
the repository's older Python scripts, which the inventory feeds into the
second release.

## Acceptance gates

1. **A real workload.** The nineteen-action notebook journey runs from a
   `.rn` file under `run`, with Python and `jq` absent, and produces the
   same package digest as the Bash harness in the same window.
2. **The session's rules, each one.** A retained closure and function value
   keep their original definition after a redefinition and a fresh call
   resolves the new one; a changed struct shape is refused and an identical
   redeclaration accepted; a compile error changes nothing; a runtime error
   after an in-memory mutation and an external write leaves both and
   publishes no binding; destructured and shadowed bindings persist; an
   infinite loop is halted with prior state intact; a refused declaration
   form names its restriction.
3. **Retained memory.** A session of one thousand inputs that retain
   closures reports its retained size, is refused past the bound with the
   message, and measures reset against the empty baseline; the cases that
   do not reclaim are listed with the reason.
4. **Cancellation on three platforms.** A child that exits nonzero, one
   that outlives its deadline with a sleeping descendant, one whose output
   exceeds the capture, and one interrupted while waited on, each produce
   the documented result on Linux, macOS, and Windows, with no child left
   behind that the platform's mechanism can reach.
5. **Diagnostics.** A compile error in a file, in an expression, and in a
   session input each report the source position; a runtime error in a
   host call carries its code.
6. **Installation and compatibility.** `cargo install --locked` on the
   three platforms produces a working binary; the pinned Rune version and
   the compatibility note are in the release.
7. **No fork.** The dependency is the published upstream crate at an exact
   version, with no patch section and no vendored copy.

## Guardrails and stop conditions

1. If a supported-input rule cannot be stated in one sentence a user can
   act on, it is not supported in this release.
2. If any session behaviour depends on replaying an earlier input, stop.
3. If a needed behaviour requires a change to the compiler or the VM, stop
   and take it upstream; do not fork.
4. If the retained-memory measurement cannot be made, the bound is not
   real and the gate is red.
5. The license and the name are not set by any commit in this repository;
   a record sets them when the operator has decided.

## Risks

- **Snapshot semantics surprise people who expect late binding.** Stated
  on first use, in the documentation, and in gate 2; the alternative,
  rebinding retained closures, is the fork this record refuses.
- **Retained memory grows.** The bound and the reset measurement are the
  mitigation; a long session that must not reset is the second release's
  problem.
- **Windows job objects and macOS process groups differ from Linux in
  ways the spike did not see.** That is why gate 4 is a release gate.
- **The host library stays small and someone writes Python anyway.** The
  inventory names what the older scripts need; the second release is
  scoped by it, not by guesswork.

## Forward

The second release: the host facilities the inventory names, the older
scripts migrated with acceptance tests, a line editor with history and
completion, and the language server wired to the session. The public
repository, once name, license, and provenance are decided.
