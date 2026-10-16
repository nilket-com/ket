# ADR 0642: shared image picker (follows adr 0631)

Decision recorded retroactively 2026-10-23 from
/home/me/work/reviews/history-cleanup-20261023/CONTRACT-r2.md
and CONTRACT-r2.1.md, with the M1/M2 preservation review.

Original design source: plans/baryon/shared-image-picker.md

## Context

Resolve terminal image capabilities once per frontend session and distribute owned clones of the resulting picker.

## Decision

- Perform detection after entering the alternate screen.
- Pass picker clones into minimap, Markdown, diff and UAST controllers; workers do not query stdio independently.
- Keep the existing halfblocks fallback and per-worker rendering ownership.

## Acceptance predicates

Assert one capability query before usable paint while retaining terminal startup/input budgets and rendering controls.

## Source and scope

This records the decision embodied at the original source checkpoints:

- a02b85cc2cd9cf666a76b18f72dbb4a73f6c5383

Relevant source components include:

- crates/baryon/src/ui/core.rs
- crates/baryon/src/ui/diff.rs
- crates/baryon/src/ui/markdown_preview.rs
- crates/baryon/src/ui/minimap.rs
- crates/baryon/src/ui/uast_graph.rs

The historical implementation checkpoints remain separate and in order.
This record does not rename, replace or modify the original ADR files.
Raw execution receipts remain external; this text claims no new test run,
production activation, completed campaign or changed acceptance status.
A new mechanism or changed decision requires a successor ADR.
