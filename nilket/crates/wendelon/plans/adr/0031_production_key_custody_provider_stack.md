# ADR 031: Production Key Custody Provider Stack

## Status
Proposed

## Context
Wendelon already committed to client-side envelope encryption for L2 data in ADR `020`, but the production Root KEK provider story remained underspecified. The planning cycle clarified three requirements:

- commodity deployments need a serious default that does not require cloud KMS or HSM hardware
- stronger custody providers must remain available as additive hardening layers
- `dev-insecure` cannot remain the de facto default for real deployments

The missing architectural surface is the provider stack itself: what kinds of Root KEK providers exist, what the abstraction boundary looks like, and what counts as a production-safe default.

## Decision

Wendelon will define a **provider-based production key custody stack** with one default serious path and two uplift paths.

1. **All Root KEK custody flows through a provider abstraction.**
   The provider contract owns operations such as:
   - generate or enroll Root KEK material
   - wrap
   - unwrap
   - rotate
   - health and readiness reporting

   Higher layers do not hard-code whether the Root KEK lives locally, in cloud KMS, or behind PKCS#11.

2. **`local-sealed` is the default serious deployment provider.**
   This is the baseline production-safe path for commodity deployments.

   `local-sealed` means:
   - Root KEK material is encrypted at rest
   - startup requires an unlock action
   - supported unlock modes are:
     - operator-supplied secret at boot
     - platform secret injection
   - the implementation may use OS facilities, sealed files, or TPM-backed local sealing internally, but those are implementation choices under the same provider contract

3. **`cloud-kms` is the first hardening uplift.**
   AWS KMS, GCP KMS, and Azure Key Vault are valid provider implementations of the same contract. This is the preferred remote-root trust model when operators want managed external custody.

4. **`pkcs11/hsm` is the enterprise uplift.**
   HSM-backed custody is supported through the same provider contract, but it is not the default or baseline assumption.

5. **`dev-insecure` is dev/test only.**
   It must be explicitly marked and must not be presented as an acceptable serious deployment provider.

6. **The baseline trust statement is explicit.**
   `local-sealed` protects against disk theft, backup leakage, and accidental exposure of stored key material. It does not claim protection against a hostile host or full root compromise of the running machine.

## Rationale

- **Commodity deployments need a real answer.** "Bring KMS or HSM first" is not a workable baseline.
- **Provider abstraction preserves the hardening path.** A clean provider seam lets Wendelon support stronger custody later without rewriting the encryption hierarchy.
- **`local-sealed` is the right default compromise.** It is materially better than plaintext or dev-only handling, while still being realistic for self-hosted and small deployments.
- **This keeps ADR `020` honest.** Envelope encryption needs a real Root KEK story; that story should not be implicitly hard-coded to cloud KMS only.

## Consequences (The "Negatives")

- **The provider contract becomes security-critical code.** Bugs in wrap/unwrap or readiness handling affect the entire custody story.
- **`local-sealed` is not equivalent to HSM or remote KMS.** Operators must understand the trust boundary.
- **The docs must clearly distinguish provider classes.** "Supported", "default", and "strongest available" are different things here.
- **Rejected alternative: cloud KMS as the only serious provider.** Rejected because it makes commodity and self-hosted deployments second-class.
- **Rejected alternative: HSM-first baseline.** Rejected because it imposes the wrong cost and operational envelope on the default product.
- **Rejected alternative: leave custody implicit inside ADR `020`.** Rejected because provider choice is a cross-cutting operational and security decision in its own right.

## References
- ADR 018: Dual-CA mTLS Identity and OID-Bound Authorization
- ADR 020: L2 Envelope Encryption and DEK Lifecycle
- ADR 026: FIPS 140-3 Certification Target and Module Boundary
