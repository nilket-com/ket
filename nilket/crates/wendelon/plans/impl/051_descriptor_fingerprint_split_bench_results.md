# ADR 051 Descriptor Fingerprint Bench Results

Bench target:

```bash
cargo bench --bench descriptor_fingerprint -- --quick
```

The bench reports isolated ADR 051 costs:

- `logical`: `compute_logical_schema_fingerprint`.
- `physical`: `compute_physical_descriptor_fingerprint(logical)`.
- `refresh`: sequential recompute of both descriptor fingerprints.
- `validate`: descriptor validation including fingerprint recompute.
- `logical-equality`: logical fingerprint / generation equality check.

The quick run is a smoke signal only. Promotion evidence should rerun the
full matrix without `--quick` after the ADR 051 code path has settled.

## 2026-04-30 Quick Smoke

Command:

```bash
cargo bench --bench descriptor_fingerprint -- --quick
```

Output:

```text
descriptor_fingerprint mode=logical shape=single-column-pk iterations=2000 total_ms=2.843 ns_per_op=1421.5
descriptor_fingerprint mode=physical shape=single-column-pk iterations=2000 total_ms=2.166 ns_per_op=1082.8
descriptor_fingerprint mode=refresh shape=single-column-pk iterations=2000 total_ms=3.879 ns_per_op=1939.5
descriptor_fingerprint mode=validate shape=single-column-pk iterations=2000 total_ms=4.123 ns_per_op=2061.6
descriptor_fingerprint mode=logical-equality shape=single-column-pk iterations=2000 total_ms=0.006 ns_per_op=2.8
descriptor_fingerprint mode=logical shape=wide-pk iterations=2000 total_ms=5.915 ns_per_op=2957.6
descriptor_fingerprint mode=physical shape=wide-pk iterations=2000 total_ms=0.382 ns_per_op=191.2
descriptor_fingerprint mode=refresh shape=wide-pk iterations=2000 total_ms=6.413 ns_per_op=3206.6
descriptor_fingerprint mode=validate shape=wide-pk iterations=2000 total_ms=23.917 ns_per_op=11958.7
descriptor_fingerprint mode=logical-equality shape=wide-pk iterations=2000 total_ms=0.003 ns_per_op=1.5
descriptor_fingerprint mode=logical shape=append-only-multirow iterations=2000 total_ms=0.790 ns_per_op=395.0
descriptor_fingerprint mode=physical shape=append-only-multirow iterations=2000 total_ms=0.407 ns_per_op=203.4
descriptor_fingerprint mode=refresh shape=append-only-multirow iterations=2000 total_ms=1.250 ns_per_op=624.9
descriptor_fingerprint mode=validate shape=append-only-multirow iterations=2000 total_ms=2.083 ns_per_op=1041.3
descriptor_fingerprint mode=logical-equality shape=append-only-multirow iterations=2000 total_ms=0.003 ns_per_op=1.5
descriptor_fingerprint mode=logical shape=content-hash iterations=2000 total_ms=0.786 ns_per_op=393.1
descriptor_fingerprint mode=physical shape=content-hash iterations=2000 total_ms=0.421 ns_per_op=210.6
descriptor_fingerprint mode=refresh shape=content-hash iterations=2000 total_ms=1.268 ns_per_op=634.1
descriptor_fingerprint mode=validate shape=content-hash iterations=2000 total_ms=2.033 ns_per_op=1016.3
descriptor_fingerprint mode=logical-equality shape=content-hash iterations=2000 total_ms=0.003 ns_per_op=1.5
descriptor_fingerprint mode=logical shape=stats-policy iterations=2000 total_ms=0.875 ns_per_op=437.4
descriptor_fingerprint mode=physical shape=stats-policy iterations=2000 total_ms=0.545 ns_per_op=272.4
descriptor_fingerprint mode=refresh shape=stats-policy iterations=2000 total_ms=1.638 ns_per_op=818.8
descriptor_fingerprint mode=validate shape=stats-policy iterations=2000 total_ms=2.712 ns_per_op=1356.2
descriptor_fingerprint mode=logical-equality shape=stats-policy iterations=2000 total_ms=0.003 ns_per_op=1.5
```

Interpretation:

- Narrow descriptor refresh stays around 0.6-2.0 microseconds/op in
  this smoke run, depending on shape and warm CPU variance.
- The wide Arrow schema dominates logical recompute and validation cost;
  physical-only recompute stays sub-microsecond.
- Physical recompute can be faster than logical recompute, especially on
  wide schemas, because it hashes the already-computed 32-byte logical
  fingerprint plus compact physical policy postcard bytes. That is the
  expected stacked-preimage behavior, not swapped axes.
- Equality checks are not a meaningful cost center relative to hashing
  and descriptor validation.
