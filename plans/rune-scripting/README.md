# Rune scripting: moved to rnx

The Rune scripting environment (a runner for `.rn` files, an expression
evaluator, and an interactive session on upstream Rune, no fork) is now its
own public project: https://github.com/nilket-com/rnx

What moved, with its history and authorship intact, on 2026-09-08:

- the feasibility spike that lived at `plans/spikes/rune-scripting/`, now the
  root of the rnx repository (ket commit 0d8af179 was its last state here);
- the first-release record that lived at
  `plans/rune-scripting/0001_the_first_release.md`, now
  `plans/0001_the_first_release.md` in rnx.

Nothing in ket depends on rnx. The nineteen-action notebook journey that the
spike reproduced is still exercised here by `uat/agent_notebook_drive.sh`.
The rnx license is a decision separate from ket and is recorded there.
