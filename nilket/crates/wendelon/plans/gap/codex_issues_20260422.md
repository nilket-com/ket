# Wendelon Unresolved Issues — Codex

Date: 2026-04-22
Author Role: Lead Architect
Sources Reviewed:
- `plans/gap/claude_answers_20260422.md`
- `plans/gap/codex_answers_20260422.md`
- `plans/gap/gemini_answers_20260422.md`

This list is revised for a solo-founder baseline: commodity hardware is the default deployment profile, no HSM is required, one operator is enough to run it, and an honest design-partner pilot comes before enterprise hardening. Better hardware is still expected to be a supported deployment profile on day 1.

## Design-Lock Required Now

1. **Default software-key bootstrap contract**

If `local-sealed` is the baseline custody path, Wendelon still needs one explicit production contract:

- operator-supplied unlock secret at boot
- platform secret injection
- or both, with one designated as the primary supported path

My recommendation remains:

- operator passphrase is the universal baseline
- platform secret injection is the automation path
- the docs must say plainly that this protects disk theft and backup leakage, not hostile-host compromise

2. **Day-1 support contract for premium hardware**

Commodity VPS is the default profile, but better hardware cannot be treated as a future compatibility surprise. The real unresolved point is what "supported on day 1" means for multi-device NVMe and higher-core-count systems.

My recommendation:

- support means correct behavior, a documented deployment path, and explicit configuration knobs
- support does not mean automatic topology tuning or benchmark-backed saturation claims yet
- storage and runtime changes must avoid assumptions that only fit the commodity profile

3. **Solo-operator control model**

The older dual-auth and break-glass debates do not fit a one-person company as the default operating model. The real issue now is what the baseline control surface looks like for one operator, while still leaving room for enterprise multi-party approval later.

My recommendation:

- baseline profile supports audited single-operator execution
- destructive and emergency actions must emit explicit audit markers and reason strings
- RBAC and approval interfaces must be designed so a future hardened profile can add dual-auth or multi-party approval without rewriting the model

4. **First honest external pilot bar**

Wendelon needs one explicit first-pilot definition. The two realistic options are:

- single-node external pilot first, then `3`-node HA later
- or require a `3`-node pilot from the start

My recommendation:

- prove single-node durability and restart safety first
- do not call that HA
- open the first serious design-partner pilot only once the `3`-node cluster path survives restart, follower loss, and bounded overload

5. **Baseline audit and archive source of truth**

The founder-stage plan cannot carry multiple partial audit stories. One source of truth is needed now.

My recommendation:

- local append-only NDJSON plus explicit archival shipping is the baseline source of truth
- Kafka stays optional and should not become a default dependency for the cheap-VPS product

## Future Hardening Only

6. **Compliance bundle shape for the hardened regulated profile**

This is still a real future scope question, but it no longer belongs on the founder-stage critical path. The remaining disagreement is whether every hardened regulated pilot bundle should always include a NIST `800-53` control-mapping summary, or whether broader framework overlays stay customer- or sector-triggered.

## External Gate Only If Pursued

7. **CMVP lab confirmation of the zeroization evidence package**

This remains external rather than architectural. It matters only if Wendelon later pursues the hardened FIPS profile.
