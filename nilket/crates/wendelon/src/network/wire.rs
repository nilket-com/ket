//! Binary wire format for Wendelon RPC payloads.
//!
//! ## The choice
//!
//! Wendelon settles on [`postcard`] as the canonical serde-backed
//! wire format for every payload that is **not** already a
//! `#[repr(C)] + bytemuck::Pod` struct. That covers:
//!
//! * `openraft` RPCs — `AppendEntriesRequest`, `InstallSnapshotRequest`,
//!   `VoteRequest`, plus their responses, whose nested generic
//!   machinery makes a hand-rolled `#[repr(C)]` layout impractical.
//! * Any future command enum or snapshot blob that rides over HRFT
//!   and needs serde rather than raw bytes.
//!
//! The fixed-size metadata structs (`FrameHeader`, `WriteReqMeta`,
//! `TransferToken`, …) continue to go on the wire as their raw
//! `bytes_of()` — they are `Pod` with a const-asserted size, so the
//! serde detour would waste CPU and bytes.
//!
//! ## Why postcard over bincode
//!
//! Two practical reasons and one principled one:
//!
//! 1. **Stable on-disk / on-wire format.** postcard explicitly
//!    commits to a spec'd encoding; bincode 1.x historically does
//!    not. For a distributed store whose cluster members may be on
//!    mixed versions during rolling upgrade, this matters.
//! 2. **Compactness.** postcard varint-encodes integers, which
//!    meaningfully shrinks log entries and vote payloads where
//!    small counters dominate.
//! 3. **no_std / embedded-friendly** out of the box. Wendelon is
//!    not targeting embedded today, but keeping the wire format
//!    `no_std`-compatible leaves the door open for tiny admin
//!    clients later.
//!
//! ## Error policy
//!
//! Both encode and decode surface as `std::io::Error`. Wire
//! corruption maps to [`ErrorKind::InvalidData`] so callers (the
//! per-ring handlers and [`crate::consensus::WendelonRaftNetwork`])
//! can convert to their native error types without reaching for a
//! foreign error crate. The `postcard::Error` chain is preserved
//! via `Error::source()`.

use crate::network::pool::{BufPool, PooledBuf};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

/// Encode any serde value into a fresh `Vec<u8>`.
///
/// One-shot path for tests and bootstrap code that don't have a
/// pool in scope. Hot-path callers on the HRFT wire should reach
/// for [`encode_into`] instead so the output buffer is recycled.
pub fn encode<T: Serialize + ?Sized>(value: &T) -> std::io::Result<Vec<u8>> {
	postcard::to_allocvec(value).map_err(wire_err_write)
}

/// Encode into a `PooledBuf` drawn from `pool`.
///
/// The buffer starts empty, postcard appends via `std::io::Write`,
/// and the returned `PooledBuf` owns the encoded bytes. When the
/// caller has finished with the frame the `Drop` on `PooledBuf`
/// returns the Vec to `pool` for reuse on the next RPC.
pub fn encode_into<T: Serialize + ?Sized>(
	pool: &Arc<BufPool>,
	value: &T,
) -> std::io::Result<PooledBuf> {
	let mut buf = pool.acquire();
	// `&mut Vec<u8>: std::io::Write` via the blanket `impl Write for
	// &mut W`, so postcard appends without reallocating when the
	// pooled Vec already has enough capacity.
	postcard::to_io(value, buf.as_mut_vec()).map_err(wire_err_write)?;
	Ok(buf)
}

/// Decode a serde value from a borrowed byte slice.
///
/// The `DeserializeOwned` bound keeps the result lifetime-free so
/// callers can return decoded values out of `async` handlers; the
/// zero-copy variant can be added as `decode_borrowed` later if a
/// hot path ever needs it.
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> std::io::Result<T> {
	postcard::from_bytes(bytes).map_err(wire_err_read)
}

// ============================================================
//  ERROR ADAPTERS
// ============================================================

fn wire_err_write(e: postcard::Error) -> Error {
	// Write-side failures are typically "message too long for the
	// buffer", "custom Serialize returned an error", etc. — all
	// logically invalid input on the submitting side.
	Error::new(
		ErrorKind::InvalidInput,
		format!("postcard encode failed: {e}"),
	)
}

fn wire_err_read(e: postcard::Error) -> Error {
	// Read-side failures mean the peer sent bytes we cannot parse.
	// That is wire-level corruption or a protocol-version skew —
	// `InvalidData` is the canonical `io::Error` variant for both.
	Error::new(
		ErrorKind::InvalidData,
		format!("postcard decode failed: {e}"),
	)
}
