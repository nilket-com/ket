# ADR 053 Wire Arrow IPC Body Compression

ADR: `plans/adr/053_wire_arrow_ipc_body_compression.md`

Use this runbook during ADR 053 rollout, when
`network_compression_*` metrics move unexpectedly, or when a
receiver reports Arrow IPC decode failures on a wire payload.

## First Look

Check these metrics over a short window, grouped by `codec` and
`path` where present:

```text
wendelon_network_compression_batches_total{codec,path,result}
wendelon_network_compression_encoded_bytes_total{codec,path}
wendelon_network_compression_encode_micros{codec,path}
wendelon_network_compression_ratio{codec,path}
wendelon_network_compression_decode_error_total{codec}
```

Expected steady state:

- `codec="lz4_frame"` dominates fresh wire encodes.
- `result="compressed"` increases with client write, migration,
  query response, and read response activity.
- `result="error"` stays zero.
- `decode_error_total{codec="lz4_frame"}` stays zero.
- `network_compression_ratio` may be sparse in production; ADR 053
  deliberately avoids double-encoding just to populate plaintext
  byte samples.

## Boundaries

ADR 053 compresses only freshly encoded Arrow IPC bodies on the
transport axis. Storage pass-through bytes are already
self-describing Arrow IPC streams under ADR 043 and are shipped
unchanged.

Fresh-encode paths counted by `network_compression_*`:

- `client_batch`
- `migration_records`
- `query_response`
- `read_response`

Pass-through storage bytes are not counted as network compression
encodes. Their storage seal/read behavior is visible under
`storage_compression_*`; a wire-side decode failure can still show up
under `network_compression_decode_error_total{codec}`.

## Decode Errors

A nonzero `wendelon_network_compression_decode_error_total{codec}` is
not a normal rollout signal. First checks:

1. Confirm the failing side was built with Arrow `ipc_compression`.
   Both root `Cargo.toml` and `crates/wendelon-client/Cargo.toml`
   must include `features = ["ipc_compression"]` for `arrow`.
2. Look for accompanying transport errors, truncation, or failed
   integrity checks. QUIC/TLS should normally detect corruption
   before Arrow sees it.
3. Check whether only one path label is seeing recent encode errors.
   Encode failures are reported as
   `network_compression_batches_total{result="error"}`.
4. Capture the span/log error text from the `StreamReader` consumer.
   Error reason is intentionally not a Prometheus label.

Escalate if decode errors continue after transport health is clean or
if both `storage_compression_decode_error_total` and
`network_compression_decode_error_total` move together for the same
codec.

## Rolling Deploy

No special order is required.

There is no HRFT envelope version bump, node capability bit,
compression flag, or negotiation window. Compressed and uncompressed
Arrow IPC bodies coexist because the IPC FlatBuffer carries the body
compression metadata. Receivers use `StreamReader` and decode both
shapes.

## Frame Flags

Only `FRAME_FLAG_STREAM_END` is allocated in ADR 053. Frames carrying
bit 0, bit 1, or any other unknown flag bit reject at
`read_frame_header` with `InvalidData`.

If this rejection appears during rollout, treat it as an incompatible
sender or corrupted frame header, not as a compression negotiation
failure. ADR 053 does not use an HRFT flag for compression.

## Interpretation

If encode time increases but encoded bytes fall materially, compare
the affected `path` against request throughput:

```text
rate(wendelon_network_compression_encoded_bytes_total{path="query_response"}[5m])
rate(wendelon_data_ring_query_requests_total[5m])
```

If encoded bytes do not fall for high-entropy or very small payloads,
that can be normal. Arrow IPC's per-buffer LZ4 path stores individual
buffers uncompressed when compression would expand them.

Identity and locality are unchanged by this ADR. Compression changes
only the byte representation of Arrow IPC bodies in flight; row
identity, routing locality, descriptor fingerprints, and storage
hashes remain defined over decoded data or existing storage bytes.
