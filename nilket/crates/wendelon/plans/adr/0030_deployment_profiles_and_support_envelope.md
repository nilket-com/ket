# ADR 030: Deployment Profiles And Support Envelope

## Status
Proposed

## Context
Wendelon is being built under two simultaneous truths:

- commodity hardware is the default deployment profile
- better hardware must be a supported deployment profile from day 1

The failure mode to avoid is architectural whiplash:

- building only for cheap VPS and discovering later that the runtime shape cannot scale onto better machines
- or building only for premium hardware and making the baseline product too expensive and operationally unrealistic

The same problem appears outside hardware as well:

- commodity self-hosted deployments vs richer operator automation
- default software-backed custody vs stronger external custody providers
- honest pilot claims vs later regulated hardening

Without an explicit support-envelope ADR, these distinctions get expressed as ad hoc exceptions, fake SKUs, or documentation drift.

## Decision

Wendelon will use a **single architecture with layered deployment profiles**, not divergent product forks.

1. **One core product, not separate architectural SKUs.**
   Wendelon ships one core runtime and one durable architecture. Differences between deployment shapes are expressed through configuration, operational posture, and support claims, not through incompatible product branches.

2. **Three deployment profiles define the support envelope.**
   - **Commodity default**
     - cheap VPS or modest bare metal
     - single node or small cluster
     - simplest documented operating path
     - default examples and first-run docs target this profile
   - **Premium supported**
     - better NVMe, more cores, multiple devices, larger boxes
     - supported on day 1 through explicit configuration
     - correctness and deployability are in scope immediately
     - full topology-aware tuning and saturation claims may come later
   - **Hardened regulated**
     - stronger custody, stronger evidence, stronger ops posture
     - additive hardening on top of the same core architecture
     - not a separate storage engine or protocol family

3. **Support and performance claims are intentionally different concepts.**
   - **Supported** means the deployment profile is expected to work correctly, has a documented path, and has explicit configuration support.
   - **Performance-claimed** means benchmarked and validated for the advertised behavior.

   A premium-hardware deployment may be supported before it is saturation-tuned.

4. **The default profile does not constrain the architecture into a dead end.**
   Core decisions must preserve:
   - clean scaling seams
   - clean custody abstraction seams
   - clean operational hardening seams

   Later uplift must be additive, not a rewrite.

5. **No documentation or marketing claim may outrun the actual supported profile.**
   Wendelon may describe future target profiles, but supported and benchmark-backed behavior must be clearly separated from aspirational design language.

## Rationale

- **This keeps the architecture honest.** The system can aim at the big leagues without pretending every hardening layer is already shipped.
- **This prevents fake SKU sprawl.** A single architecture with layered support profiles is easier to reason about than inventing a "small" product and an "enterprise" product that later need to merge.
- **This protects premium hardware without overcommitting to it.** Better hardware is a real supported target from day 1, but that does not force premature performance claims.
- **This protects commodity deployment from being treated as a second-class hack.** Commodity default is the default because it is operationally realistic, not because it is disposable.

## Consequences (The "Negatives")

- **Support matrices become part of the architecture.** The team must be explicit about what is merely expected to work vs what has been benchmarked and validated.
- **Some operator questions will get "supported but not yet tuned" as an answer.** That is acceptable and more honest than inventing unproven claims.
- **Later hardening work must preserve backward compatibility where practical.** A layered profile model makes gratuitous forks harder to justify.
- **Rejected alternative: separate cheap and enterprise architectures.** Rejected because it creates long-term protocol and storage debt.
- **Rejected alternative: premium hardware only.** Rejected because it would make the product unrealistic to develop and operate early.
- **Rejected alternative: commodity only until much later.** Rejected because it would risk baking in assumptions that block later scale.

## References
- ADR 009: `io_uring` Execution Topology
- ADR 011: Transport Support Strategy (QUIC First, Multi-Transport Later)
- ADR 018: Dual-CA mTLS Identity and OID-Bound Authorization
- ADR 020: L2 Envelope Encryption and DEK Lifecycle
