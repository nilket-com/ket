# ADR 0004: `ket` is the product router

Status: accepted 2026-08-01. It records the product-wide command grammar
anticipated by TORON-532 without changing Nilket's role/profile grammar or
either editor head's CLI.

## Context

The unified repository contains five user-facing nouns:

- Baryon, the focused editor;
- Polariton, the data workbench;
- Nilket, the database deployment;
- Toron, Nilket's compute role; and
- Wendelon, Nilket's storage role.

They are one product state vector but are entered through separate binaries
and installation knowledge. The repository name already supplies the missing
product noun: `ket`. A product front door should make every surface easy to
name without becoming a sixth runtime, duplicating Nilket's supervisor, or
teaching product logic to branch on an interchangeable analytics engine.

TORON-532 deliberately left this decision to a future PRODUCT record. It also
established the grammar this router must preserve: `nilket` is a deployment;
`toron` and `wendelon` are expert role nouns. The Rust source adds one important
fact: Toron has no standalone installed binary. Its public operator surface is
`nilket toron ...`. Routing a role directly would therefore invent a second
CLI and bypass the owner of the role grammar.

## Decision

### 1. `ket` is a transparent product router

The stable routes are:

```text
ket                         -> configured default (factory: Baryon)
ket baryon [ARGS...]        -> baryon [ARGS...]
ket polariton [ARGS...]     -> polariton [ARGS...]
ket nilket [ARGS...]        -> nilket [ARGS...]
ket toron [ARGS...]         -> nilket toron [ARGS...]
ket wendelon [ARGS...]      -> nilket wendelon [ARGS...]
```

On supported Unix platforms the router resolves the leaf executable and uses
process replacement. It does not spawn and wait, daemonize, supervise, proxy
standard streams, translate exit statuses, or retain a router PID. The target
inherits the current working directory, environment, standard streams, and
signals. `ket` disappears from the process tree.

V1 is Linux-only, matching the platform exercised by product CI. Other
platforms receive an honest build, package, or startup refusal until they can
preserve this contract; they may not silently substitute spawn-and-wait while
claiming transparent routing.

Arguments are forwarded byte-for-byte and in order. No shell participates.
The Toron and Wendelon routes prepend exactly one role noun before forwarding
the remaining arguments to `nilket`; they do not interpret role options.
Consequently `ket toron` with no role verb has the same help/refusal semantics
as `nilket toron` and never starts Wendelon implicitly.

`ket --help` and `ket --version` belong to the router. An explicit route is the
escape hatch for forwarding those flags to a target, for example
`ket baryon --help`. The route nouns and `default` are reserved in first-argument
position; a colliding filename is passed explicitly through the desired head.
All other arguments to bare `ket` are forwarded to the configured default, so
the factory configuration preserves editor use such as `ket path/to/file`.

### 2. Bare `ket` has a visible, typed default

The factory default is Baryon. Absence of user configuration means Baryon; it
does not cause a config file to be written merely by launching the product.

The persistent override is a small, versioned, inspectable artifact under
`${XDG_CONFIG_HOME:-$HOME/.config}/ket/`. It stores a typed `DefaultTarget`,
not an arbitrary executable name or command string. The management grammar is:

```text
ket default                             # show target, source, and config path
ket default baryon|polariton|nilket     # atomically set the override
ket default reset                       # remove the override; factory Baryon wins
```

Only a router-owned, recognized config artifact may be reset. An unsupported
version, malformed file, unknown key, or invalid target is an actionable
refusal; the router never silently falls back to Baryon after observing an
invalid explicit choice. The artifact is configuration, not secret state, and
may be inspected or versioned by the user.

Changing the default does not rename or weaken explicit routes. It selects
only what bare `ket [ARGS...]` means.

### 3. Default eligibility encodes product grammar

`DefaultTarget` is a closed set containing only:

```text
Baryon | Polariton | Nilket
```

Baryon and Polariton are applications; Nilket is a supervised deployment.
Toron and Wendelon are raw roles and are deliberately ineligible. A role is an
expert noun addressed explicitly, not a place entered accidentally through a
one-word product door.

The type boundary is enforced independently of CLI validation: no generic
string-to-command escape hatch and no conversion from every explicit route to
`DefaultTarget` exists. Negative tests prove that `ket default toron` and
`ket default wendelon` refuse, with a positive control proving each remains
available as an explicit route.

Choosing Nilket as the default is an explicit user decision. Bare `ket` then
inherits Nilket's zero-argument local-deployment behavior from TORON-532. This
does not make Toron the product-wide default by accident, does not award any
promotion-gate credit, and does not give the router lifecycle ownership.

### 4. Target resolution is deterministic and honest

For the leaf executable selected by the route, resolution is:

1. inspect the expected sibling of the running `ket` binary: if absent,
   continue; if present, it must be an executable regular file that is not the
   router itself, or routing refuses; then
2. search the operating system's `PATH` for the executable.

Sibling-first resolution keeps binaries from one package or `cargo install`
prefix coherent. `PATH` preserves ordinary development and system-package
use. The router neither searches the repository nor guesses `target/`
directories, Cargo profiles, package-manager paths, or aliases.

An invalid present sibling never falls through to a different installation on
`PATH`; that would defeat package coherence. `PATH` traversal follows platform
executable-search rules and rejects a candidate that resolves back to the
router itself. A missing target names the logical route, the required leaf
executable, both locations searched, and an install hint. It never falls
through to another head or converts a missing role route into local Nilket
startup.

The route table is the single authority for both execution and help output, so
the two surfaces cannot drift. Tests cover sibling precedence, PATH fallback,
missing-target refusal, self-cycle refusal, exact argument forwarding, and the
two Nilket role prefixes.

### 5. The router owns no product semantics

The router has no dependency on libaryon, either editor head, Toron, Wendelon,
their clients, Arrow, Rune, or connector crates. It does not inspect workspace
capabilities, database health, project files, or editor configuration. It
never starts Nilket while routing to Polariton and never stops a deployment.

This is the executable meaning of allowing the product name `ket` at the
composition boundary without violating PRODUCT-0002's capability-over-brand
rule. Backends remain capabilities behind the workbench; `ket` names the frame
from which those capabilities are selected.

The first implementation is deliberately small and dependency-free apart
from the standard library and platform process APIs. Architecture guards with
synthetic positive controls reject product-crate dependencies and duplicated
route/default tables.

## Rejected

- **Fixing bare `ket` permanently to Baryon:** confuses a useful factory
  choice with product constitution and prevents the visible default requested
  by lived use.
- **Making every explicit route eligible as the default:** permits a raw
  compute or storage role behind an ambient product door, discarding Nilket's
  supervision grammar.
- **Resolving `ket toron` to a standalone `toron` binary:** no such installed
  product surface exists, and creating one would duplicate Nilket's role
  parser. The route is `nilket toron` by construction.
- **Treating `ket nilket` as shorthand implemented inside the router for
  `nilket local`:** duplicates Nilket's default policy. The router forwards to
  `nilket`; Nilket alone decides what its zero-argument invocation means.
- **Having `ket polariton` auto-start Nilket:** introduces process ownership,
  teardown, and failure policy into a transparent route. A future composed
  session requires its own evidence and record.
- **Spawning and supervising the target:** adds a PID layer, changes signal
  and exit semantics, and risks duplicating TORON-532's carefully bounded
  supervisor.
- **A shell-based router:** makes quoting and argument identity platform- and
  input-dependent and expands the security surface for no product benefit.
- **PATH-only or repository-search resolution:** the former can mix package
  generations; the latter turns build-tree accidents into user-visible
  behavior.
- **Arbitrary commands in the default artifact:** changes a typed product
  choice into a shell launcher and makes a malformed or hostile config part of
  product startup.
- **Brand checks in Baryon, Polariton, or libaryon:** the router is a command
  surface, not permission for product logic to branch on engine identity.

## Consequences and sequencing

- The repository gains one canonical front door without collapsing the five
  existing nouns or their ownership boundaries.
- Explicit commands remain stable regardless of user preference; only bare
  `ket` is configurable.
- Two one-word doors compose honestly: factory `ket` enters Baryon, while a
  user-selected Nilket default enters the supervised local database. Raw roles
  always require their names.
- `ket local` remains unassigned. Whole-product sessions, Polariton-plus-local-
  Nilket ownership, health-aware attachment, and typed startup profiles beyond
  the single default are future decisions, not aliases smuggled into this one.
- Implementation receives its own plan and must include route-table,
  eligibility, config-corruption, resolution, argument-identity, and
  architecture-guard positive controls before the router becomes an install
  recommendation.
- Packaging must install `ket` alongside the heads it advertises or document
  their independent installation. The router's honest missing-target refusal
  remains required even in bundled distributions.
