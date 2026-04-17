# Red Flags in Teammate Answer Documents
**Date:** 2026-04-17
**Reviewer:** Claude
**Last updated:** round 3, after re-reading all three answer sets end-to-end
**Sources reviewed:**
- [codex_answers_20260417.md](/home/me/work/wendelon/plans/gap/codex_answers_20260417.md)
- [gemini_answers_20260417.md](/home/me/work/wendelon/plans/gap/gemini_answers_20260417.md)

The bar for "red flag" is **a decision that would actively harm Wendelon if adopted** — not a decision I'd personally have made differently. Stylistic disagreements are not in this document.

---

## Status of prior red flags

**Gemini's first-pass document** had ~12 critical/serious structural red flags (discarded `openraft`, bespoke FIPS Level 4 module, PMEM/CXL durability, SGX DEK caching, homomorphic encryption, blockchain audit anchoring, compile-time OOM proofs, TLA+-as-runtime-checker, ZNS application compaction, eBPF/XDP authz, WebAssembly-delivered CLI). **The revised document retracts all of them.**

**Codex's first-pass document** had four "worth revisiting" items: cipher choice, encryption granularity, SSE-KMS peer-status, pending-upload framing. **Round 2 resolved all four**, and **round 3 further softened** Codex's §3 snapshot thresholds and §4 multipart threshold — both of which I previously flagged as aggressive and under-specified respectively. Codex now frames snapshot thresholds as benchmark-derived and multipart threshold as "nominal starting point around 64 MiB, final tuning by benchmark."

---

## Current concerns — Gemini's revised answers

No critical red flags. Three remaining refinement points:

1. **§6 stays at "OID extension on a single trust root."**
   The revised answer still doesn't adopt the dual Node-CA / Operator-CA separation that Codex, Claude, and the existing `plans/ops/multi_tenancy.md` converged on. Functionally OID-only works, but revoking operator authority in an incident becomes a CRL hunt instead of a one-action CA revocation. Minor; recommend alignment.

2. **§3 three-trigger snapshot policy (log count + wall-clock + state-growth delta) is underspecified for trigger interaction.**
   If state grows faster than segments rotate, the growth trigger can fire between WAL segment boundaries, which interacts oddly with segment-based compaction. A one-line rule ("growth trigger fires at the next segment boundary at or above the threshold") resolves it. Not a blocker.

3. **§8 "custom deterministic harness explicitly built around `io_uring` and `quinn`."**
   Building around `quinn` deterministically is harder than the sentence implies — `quinn` has its own internal clocks, timers, and OS-socket dependencies. The turmoil-style approach (replace the UDP socket under `quinn`) is the known-working path. Custom is viable, just more expensive than stated.

## Current concerns — Codex's round-3 answers

No blocking concerns. One small carryover:

1. **§5 plain-GCM fallback relies on "a nonce scheme whose uniqueness follows directly from the data model."**
   This is the same fallback branch Gemini's issues file flagged as fragile. Codex has built in the right safety valve ("If that construction cannot be specified and audited cleanly, the feature should block rather than ship on hand-waving"), and my own §5 now ties plain-GCM fallback exclusively to the FIPS-and-SIV-unavailable case. Three answer sets agree the risk is real and gated by ADR-time verification — this is a watch item, not a red flag. Just worth restating so it doesn't quietly un-gate later.

Previously flagged `§3 snapshot thresholds` (10k/5min/64 MiB) and `§4 multipart 128 MiB fixed` — **both resolved in round 3.** Codex's snapshot thresholds are now benchmark-derived with a tens-of-thousands to low-hundreds-of-thousands entry range, matching my own band. Multipart is now a nominal 64 MiB starting point with benchmark tuning, no longer a fixed 128 MiB.

---

## Summary

- **Gemini's revised answers** — a serious engineering document. Three minor alignment notes above; no architectural red flags.
- **Codex's round-3 answers** — no blocking concerns; two prior tuning disagreements resolved by Codex's softening.
- **Claude's (my own) revisions** — adopted dual trust roots (§6), full 140-3 certification endpoint (§6), redb (§9), tenant-aware-from-day-one (§10), explicit Raft-authoritative-lifecycle / local-execution-ledger split (§4), and a reconciled cipher fallback decision tree (§5) after Gemini's fragility flag.

After three rounds, the three answer sets have converged tightly. Remaining disagreements are narrow and benchmark-resolvable. The architecture position is stable — this is the point at which the team should stop circulating answer revisions and start writing ADRs.
