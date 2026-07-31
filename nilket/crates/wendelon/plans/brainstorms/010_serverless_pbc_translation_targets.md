# Brainstorm 010: Serverless PBC Translation Targets

## Preconditions
This brainstorm does not open for implementation work until:
1. The vectorized executor from ADR 035 has landed.
2. A concrete consumer of PBC capability negotiation exists (the ADR 036 version/capability mechanism is actually used in anger by Toron).

Until both are true, this is scoping-and-thinking material, not a work plan.

## The Premise

Wendelon's L2 tier stores cold slabs in object storage. If Toron queries years of historical data and Wendelon downloads entire cold objects just to discard most rows, the architecture loses its central advantage: moving compute intent to data instead of moving data to compute.

PBC is already a clean, serialized AST of vectorized operations (after ADR 035). It doesn't have to run only on Wendelon's L1 NVMe nodes. It could be the foundational mechanism for querying the L2 cold-storage tier too — either by translating it to a provider-native filter language or by executing it in a Wendelon-shipped runtime that sits next to the object store.

The long-term goal is for Toron to send one typed query plan, have Wendelon split it across L1 and L2 placement, and execute the same constrained pushdown semantics wherever the data lives.

## Why this is a separate brainstorm

L2 pushdown introduces concerns that the local executor change deliberately avoided:

- object format compatibility
- encryption and key access (interacts with ADR 020)
- provider-specific capabilities
- deployment of remote runtimes
- audit and tenant isolation outside the Wendelon process (interacts with ADR 021 and ADR 025)
- cost and egress behavior

None of those apply to the L1 executor. Bundling them into one ADR would have slowed the highest-leverage win.

## Candidate execution targets

### Target A: Local L1/L2 Materialized Execution
Wendelon fetches or reads an Arrow batch, decrypts/verifies it if needed, and runs the vectorized PBC interpreter in-process. This is the safe default once ADR 035 lands — it gives semantic parity for cold data even before any provider-adjacent compute exists.

### Target B: Provider-Native Predicate Translation
For a narrow expression subset, Wendelon may translate PBC (or the upstream `toron-ir` fragment) into a provider-native predicate format such as S3 Select SQL.

This is only valid when:
- the object format is supported by the provider-native filter
- encryption/authentication semantics remain valid
- the predicate is exact, not approximate
- the provider capability is detected and versioned
- fallback to Target A is automatic

Treat this as an optional adapter, not the core design.

### Target C: Wendelon Remote Pushdown Runtime
Wendelon ships a small Rust runtime near object storage (AWS Lambda via S3 Object Lambda, Cloudflare Worker, or similar). The runtime:
- receives a signed, bounded PBC program
- fetches authorized slab/object ranges
- verifies and decrypts only what it is allowed to read
- runs the same vectorized interpreter over Arrow batches
- returns filtered/projected Arrow IPC to Wendelon or Toron

This is the more general long-term path because it preserves Wendelon semantics instead of depending on each object provider's predicate language. It also bypasses the Wasm 4 GB memory-limit trap entirely because the workers execute native Rust interpreting PBC directly against raw Arrow memory.

## Capability model

Future L2 pushdown capability responses should expose at least:

- `pbc_version`
- `supported_opcodes`
- `supported_arrow_types`
- `supports_projection`
- `supports_nullable_masks`
- `supports_provider_native_translation`
- `supports_remote_runtime`
- `max_program_bytes`
- `max_batch_bytes`
- `max_result_bytes`
- `tenant_crypto_mode`

Toron never assumes L2 pushdown exists because L1 vectorized PBC exists. It plans from capabilities.

## Security and compliance constraints

- Remote runtimes never receive broad tenant credentials. They receive bounded, short-lived capability to read specific slab objects or byte ranges.
- Any decrypted data in provider-adjacent compute must be covered by the same audit and tenant policy story as in-process Wendelon reads (ADR 025).
- Provider-native filters cannot operate over ciphertext unless the provider supports a compatible encrypted format. Slab encryption (ADR 020) is not weakened to make pushdown easier.
- Result integrity must be authenticated. Wendelon or Toron must be able to detect truncated or tampered remote-runtime output.

## Likely sequencing

1. Land the vectorized executor (ADR 035).
2. Add the first real PBC version/capability consumer that exercises ADR 036's negotiation path.
3. Open implementation work on this brainstorm with concrete Toron L2 query patterns in hand.
4. Phase: Target A local materialization first (pure reuse of the in-process interpreter).
5. Phase: pick one provider-native translation target (Target B) and spike semantic parity against a single object format; accept or reject the adapter.
6. Phase: design ADR for remote runtime (Target C) covering packaging, signing, authentication, tenant key access, audit events, resource limits, result framing, and trace-context propagation.
7. Phase: provider-neutral prototype of the remote runtime as a local process. Byte-for-byte parity against in-process vectorized PBC before any cloud binding.

## Risks and open questions

- **Provider lock-in.** Keep provider-native translation behind adapters and prefer the Wendelon remote runtime for generality.
- **Semantic mismatch.** Require exact parity tests. Approximate pushdown must be labelled approximate and only used for planner-approved prefiltering (ADR 036 `PushdownApprox`).
- **Key leakage.** Use short-lived scoped credentials and avoid shipping tenant KEKs directly to generic serverless code.
- **Cost surprises.** Add metrics for bytes scanned remotely, bytes returned, runtime duration, and fallback rate before making claims.
- **Operational complexity.** Do not block any L1 workstream on remote-runtime work.

Open questions the implementation ADR will need to answer:

- Who receives remote results first? Wendelon can enforce storage policy centrally; Toron-direct return may avoid an extra hop.
- Can encrypted slab layout support provider-native filtering at all? If not, Target B is limited to unencrypted test profiles or rejected outright.
- What is the first provider target? Choose based on local development speed and semantic fit, not cloud marketing value.
- Should remote-runtime deployment be operator-managed or Wendelon-managed? Auto-deploying cloud functions crosses a major operational boundary.

## The potential payoff

By serializing compute intent (PBC) instead of just pulling bytes, Wendelon could turn object storage into a massive, per-tenant-scoped serverless compute grid. A developer running Wendelon on a single small VPS could execute queries against cold data by leveraging the cloud provider's internal infrastructure for the heavy lifting. That is the long-horizon architecture; this brainstorm is the placeholder for getting there responsibly.
