# Rune scripting and REPL feasibility spike

Status: spike executed 2026-09-08. This is a decision checkpoint, not a product
commitment or a new public project's API. No compiler fork is assumed.

Outcome and limitations: [evidence.md](evidence.md). The prototype executes the
real 19-action journey from Rune and demonstrates a constrained persistent REPL
without a compiler fork. It is not a production scripting environment.

## Reproduce

This is an independent, unpublished Cargo package pinned to Rune 0.14.1, with
its own lockfile. It uses the compiler/VM APIs directly; it does not instantiate
Baryon, Polariton or Toron and does not yet wrap upstream `rune::cli::Entry`.
The host process implementation is Linux/Unix only.

```sh
cargo run --locked --manifest-path plans/spikes/rune-scripting/Cargo.toml
cargo run --locked --manifest-path plans/spikes/rune-scripting/Cargo.toml -- eval 'let x = 4; x + 3'
cargo run --locked --manifest-path plans/spikes/rune-scripting/Cargo.toml -- repl
```

The default command runs assertions and prints observations. The REPL accepts
single-line inputs, `:begin`/`:end` multiline input, `:reset` and `:quit`.
It has no line editor or completion. It supports a deliberately limited set of
persistent declarations and bindings; unsupported declarations refuse rather
than pretending to survive. Successful inputs publish bindings/declarations;
failed inputs can still mutate shared values and perform external effects.

For the real-window exercise, start one fresh `polariton --agents`, take its id
from `polariton windows`, and provide a new output directory whose parent exists:

```sh
cargo run --locked --manifest-path plans/spikes/rune-scripting/Cargo.toml -- \
  run plans/spikes/rune-scripting/journey.rn \
  /absolute/path/to/polariton WINDOW_ID /absolute/path/to/data.parquet \
  /tmp/new-rune-journey first-data first-plot
```

The `.rn` file uses only generic host modules and the public `polariton act`
protocol. It saves commands and answers, requires a new incarnation after each
plot run, and stops on unknown/failure without replay. Close the test window
afterward. Bash/jq remains the product acceptance harness until a supported Rune
runner is deliberately adopted; this spike is not a new install dependency.

The immediate Python harness replacement is independent and uses Bash/jq.
This spike asks whether a standalone Rune host can replace that orchestration,
and what persistent interactive evaluation can truthfully guarantee.

## Questions that must be executed

1. Compile independent units, transferring Rune values without replaying old
   statements. Retain a closure and a struct instance; redefine a function and
   record which definition each call uses. Change the struct shape as a negative
   control. Keep a closure alive after its original VM has been dropped.
2. Introduce a compile error, then a runtime error after a mutation. Observe
   binding publication, aliased state and external effects separately. Do not
   promise rollback of already executed work.
3. Use Rune's AST to separate declarations/statements and identify bindings,
   including shadowing and destructuring. An expression wrapper alone is not a
   persistent REPL.
4. Expose files, JSON, argument-array subprocesses, bounded capture and deadlines
   in a standalone host. Run the same 19-action notebook journey without Python.
   Exercise process failure, timeout and cancellation rather than only success.

The resulting report must separate demonstrated behavior, unsupported behavior,
and production work remaining. A runner may be viable even if the proposed REPL
state model fails. Public repository placement follows this evidence.

## Python inventory

All four older tracked Python files import only the standard library:

| File | Workload | Host facilities needed |
|---|---|---|
| `plans/product/migration/verify_nilket_graft.py` | Frozen Git migration audit | Paths, binary subprocess pipes, byte parsing, maps |
| `nilket/scripts/classify_fold_perf.py` | Perf-symbol classification | Process execution, JSON, regular expressions, counters |
| `nilket/crates/toron/scripts/bench/summarize_runtime_filter_perft.py` | Benchmark summaries and comparisons | File/text parsing, grouping, arithmetic, formatted output |
| `nilket/crates/wendelon/ci/seed_retention_advisory.py` | CI retention advisory | JSON, TOML, dates, environment, HTTP and URL handling |

This is not a migration of those files. Standard-library-only still includes
substantial behavior: streaming binary pipes, HTTP error handling and dates need
proper replacements. The new notebook harness is the initial, narrower workload.

Upstream baseline: the repository resolves Rune 0.14.1. Its `rune::cli::Entry`
supports custom contexts and script/check/test/format/documentation commands;
its CLI command enum contains no REPL. Existing Baryon job bindings are useful
reference material, but the standalone spike must not instantiate an editor.
