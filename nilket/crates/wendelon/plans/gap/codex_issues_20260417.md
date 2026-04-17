# Codex Red Flags on Teammate Answers
**Date:** 2026-04-17

This file reflects a fresh read of the current answer documents after the latest answer-round, not the earlier drafts. The threshold for inclusion remains the same: only decisions that would actively harm Wendelon if adopted belong here.

## Current Findings
- None at the red-flag threshold.

## Resolved Since Prior Review
- Claude resolved the pending-upload concern by switching to a Raft-authoritative lifecycle plus bounded local execution ledger in [claude_answers_20260417.md](/home/me/work/wendelon/plans/gap/claude_answers_20260417.md:84).
- Claude also resolved the earlier multi-tenancy timing conflict by requiring tenant-aware architecture from day one in [claude_answers_20260417.md](/home/me/work/wendelon/plans/gap/claude_answers_20260417.md:244).
- Claude's latest round also resolved the remaining crypto and multipart ambiguity by adding an explicit cipher fallback decision tree and by splitting multipart trigger threshold from multipart part size in [claude_answers_20260417.md](/home/me/work/wendelon/plans/gap/claude_answers_20260417.md:98) and [claude_answers_20260417.md](/home/me/work/wendelon/plans/gap/claude_answers_20260417.md:115).
- Gemini’s revised draft no longer contains the earlier consensus rewrite, bespoke hardware durability assumptions, self-built FIPS module, or invented L2 control-plane subsystems that previously made the document unsafe to use. The current version is materially closer to the deployable architecture discussed by the team.

## Notes
- Gemini's current draft still leaves a few choices less pinned down than I would like, such as `redb` versus another embedded index store and single-CA OID claims versus explicit trust-root separation, but those are refinement gaps rather than harmful decisions.
- I still disagree with some choices and degrees of certainty in the teammate documents, but those disagreements are now ordinary architectural preferences, not significant red flags.
