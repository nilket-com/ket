# ADR 014: RustFS as the S3 Test Backend

## Status
Accepted

## Context
Wendelon's L2 tier is defined behind the `ObjectStore` trait (ADR 006) and the
production target is a real S3-compatible service. Integration tests for the
sweeper, promotion, and `read_from_l2` paths need an S3 endpoint the test
harness can start and tear down locally; a pure mock is not sufficient because
we want to exercise real HTTP, multipart behavior, and error semantics.

The natural default would have been MinIO, which has been the de-facto local
S3 test service for years. In 2026 MinIO relicensed away from its prior
open-source posture to focus on a commercial path, which makes it a poor fit
for an open infrastructure project's test dependency.

We need a local S3-compatible service that:
- is open-source under a license compatible with Wendelon's,
- implements enough of the S3 API to cover our `ObjectStore` surface (put,
  get, delete, list, multipart),
- can be launched and torn down cheaply from a test harness,
- is stable enough to not introduce test flakiness.

## Decision
Integration tests for the L2 `ObjectStore` (notably `tests/l2_s3.rs` and
anything else exercising the S3 path) will run against **RustFS**. The
production `ObjectStore` implementation remains a generic S3 client and is
not coupled to RustFS; only the test harness depends on it.

## Rationale
- **License alignment.** RustFS is open-source and suitable for an open test
  dependency. MinIO no longer is.
- **Protocol parity.** RustFS's S3 layer is near-parity with MinIO for the
  operations `ObjectStore` actually uses. A RustFS-passing test should
  behave identically against AWS S3 or any other compliant backend.
- **Maturity match.** RustFS is in alpha with a 1.0 target roughly three
  months out. Wendelon is itself pre-alpha, so pairing with an alpha
  dependency that reaches 1.0 before we do is acceptable.
- **Stack consistency.** RustFS is itself written in Rust, which keeps the
  test stack single-language and the test harness free of JVM/Go
  cross-dependencies.
- **Swappability.** The `ObjectStore` trait isolates the backend. If RustFS
  regresses, slips 1.0, or changes direction, we can swap to another
  compliant service (SeaweedFS, Garage, or direct AWS for cloud CI) by
  changing the test harness only. No production code changes required.

## Consequences (The "Negatives")
- **Alpha dependency.** RustFS has not yet reached 1.0; bugs or API shifts
  during the alpha window may cause transient test flakiness. We accept
  this as a time-bounded risk.
- **Smaller ecosystem than MinIO.** Less prior art for debugging edge cases.
  If a test fails in a way that looks S3-spec-ambiguous, we own the
  investigation rather than reaching for community knowledge.
- **Extra CI moving part.** The CI image needs RustFS installed and a
  harness that can start and stop it reliably. Mitigation: wrap in a
  `tests/support/rustfs.rs` helper so individual tests do not re-implement
  lifecycle.
- **Revisit trigger.** If RustFS 1.0 slips past its 3-month target by more
  than one additional quarter, or if a concrete S3 compatibility gap blocks
  a test we need, we re-open this decision. The revisit is cheap because
  the swap is confined to the test harness.

## Related
- ADR 006 (Tiered Storage and Lazy Migration) — defines the `ObjectStore`
  trait this decision tests.
