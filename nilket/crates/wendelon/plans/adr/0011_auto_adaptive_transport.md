# ADR 011: Transport Support Strategy (QUIC First, Multi-Transport Later)

## Status
Accepted (amended 2026-04-22)

## Context
Wendelon wants a transport story that can grow from commodity deployments to very high-end hardware. The original version of this ADR expressed that ambition as an auto-adaptive multi-transport architecture with RDMA, QUIC plus `io_uring`, and TCP/TLS fallback.

The planning and code-review cycle made two facts obvious:

- the real day-1 implementation is QUIC-based, not multi-transport
- leaving the ADR in an aspirational state encourages the codebase and docs to over-claim what exists

The correct architectural move is not to abandon future transport extensibility. It is to separate:

- the **supported day-1 transport**
- the **required overload and admission behavior**
- the **future extension point for additional transports**

## Decision

Wendelon's supported transport strategy is **QUIC first**.

1. **QUIC is the supported day-1 transport for both commodity and premium hardware profiles.**
   The baseline transport is `quinn` over QUIC with mTLS and the existing framing model.

2. **The transport layer keeps an abstraction seam, but no adaptive transport claim is made until more than one real transport exists.**
   Wendelon may preserve an internal `TransportProvider`-style boundary so additional transports can be added later, but:
   - transport negotiation
   - capability advertising
   - RDMA upgrades
   - transport-tier fallback claims

   are not part of the supported architecture until they are actually implemented.

3. **Day-1 transport support includes bounded admission control.**
   The supported QUIC path must provide:
   - max total connections
   - max connections per peer/IP
   - max streams per connection
   - bounded task fan-out
   - post-auth fairness and backpressure

4. **Transport claims must be truth-matched to implementation.**
   If a tuning path such as `io_uring` integration, GSO, or future premium-fabric transport is not implemented and benchmarked, it is not part of the supported transport claim.

5. **Future transport work remains allowed but explicitly staged.**
   Additional transports such as RDMA or a more specialized premium-hardware path may be added later if and only if they arrive with:
   - a real implementation
   - real benchmarks
   - real failure-mode coverage
   - an honest support contract

## Rationale

- **QUIC is the real universal transport today.** It works across commodity VPS, modest bare metal, and better hardware without introducing a matrix of half-built transport promises.
- **Support is more valuable than aspiration.** A truthful QUIC-first contract is better than a fictional auto-adaptive transport stack.
- **Admission control is part of transport correctness.** A transport that accepts unbounded streams or task fan-out is not merely untuned; it is operationally unsafe.
- **Future extensibility still matters.** Preserving a clean internal boundary now keeps the premium-hardware path open later without forcing Wendelon to pretend that path already exists.

## Consequences (The "Negatives")

- **The transport story is less glamorous in the short term.** Accepted because architectural honesty matters more than vision-language in the ADRs.
- **Premium-hardware transports are explicitly deferred.** Accepted because "supported later" is better than "implied now."
- **Internal abstraction may precede external capability.** Accepted because a narrow internal seam is cheap insurance against later rewrites.
- **Rejected alternative: advertise adaptive multi-transport now and fill it in later.** Rejected because it creates documentation debt and misleads future implementation work.
- **Rejected alternative: collapse everything to a transport-specific codebase with no abstraction seam.** Rejected because it would make later premium-hardware support more expensive than it needs to be.

## References
- ADR 004: Zero-Copy Custom Transport (Rejecting gRPC/Protobuf)
- ADR 008: Dependency Selection and Async Runtime Model
- ADR 018: Dual-CA mTLS Identity and OID-Bound Authorization
