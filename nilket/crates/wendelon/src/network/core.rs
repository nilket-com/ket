//! HRFT wire frame framing.
//!
//! Every Wendelon RPC — Data Ring, Control Ring, Raft internal —
//! rides one `HrftFrame` per bidi stream. The frame is laid out as:
//!
//! ```text
//!   [24-byte FrameHeader] [meta bytes] [arrow bytes?] [trailing bytes?]
//! ```
//!
//! `FrameHeader` is a `#[repr(C)] + bytemuck::Pod` struct so it is
//! read and written as raw bytes without any (de)serialization.
//! The three variable-length sections are pulled off the wire into
//! [`PooledBuf`]s drawn from a shared [`BufPool`]. That gives the
//! framing layer allocation-minimal steady-state reads after warmup;
//! it is not strict zero-copy because the transport still copies
//! bytes into those pooled `Vec<u8>` buffers.

use crate::network::pool::{BufPool, PooledBuf};
use crate::network::wire;
use crate::schema::{FRAME_FLAG_KNOWN_MASK, FrameHeader};
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const HRFT_MAGIC: u32 = 0x48524654; // "HRFT" in ASCII

/// Upper bound on any single payload section. Mirrors the
/// `max_frame_size` knob in `plans/ops/configuration.md` (5 MiB
/// default, expressed here per-section since meta + arrow +
/// trailing can each be up to that cap).
pub const MAX_SECTION_BYTES: u32 = 5 * 1024 * 1024;

/// A fully parsed Wendelon wire frame.
///
/// All three payload sections are held in [`PooledBuf`]s so they
/// return to the originating [`BufPool`] when the frame is dropped.
/// Accessors derive transparently via `Deref<Target = [u8]>`, so
/// code that used to take `&[u8]` against `&frame.meta` keeps
/// working without changes.
#[derive(Debug)]
pub struct HrftFrame {
	pub header: FrameHeader,
	pub meta: PooledBuf,
	pub arrow_payload: Option<PooledBuf>,
	pub trailing: Option<PooledBuf>,
}

/// Reads a complete HRFT frame from an asynchronous stream (e.g.
/// `quinn::RecvStream`), drawing payload buffers from `pool`.
///
/// The returned frame's `meta`, `arrow_payload`, and `trailing`
/// sections all live in pooled Vec<u8>s; dropping the frame returns
/// them for reuse on the next stream.
pub async fn read_frame<R: AsyncRead + Unpin + ?Sized>(
	reader: &mut R,
	pool: &Arc<BufPool>,
) -> std::io::Result<HrftFrame> {
	let header = read_frame_header(reader).await?;
	read_frame_from_header(reader, pool, header).await
}

/// Reads and validates just the fixed-size HRFT header.
///
/// Callers that need to decide how to consume the rest of the
/// stream (for example, bulk client-streaming RPCs) can peek the
/// message type first and then hand the validated header to
/// [`read_frame_from_header`] to finish the frame.
pub async fn read_frame_header<R: AsyncRead + Unpin + ?Sized>(
	reader: &mut R,
) -> std::io::Result<FrameHeader> {
	let mut header_buf = [0u8; std::mem::size_of::<FrameHeader>()];
	reader.read_exact(&mut header_buf).await?;

	let header: &FrameHeader = bytemuck::from_bytes(&header_buf);
	if header.magic != HRFT_MAGIC {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"invalid HRFT magic bytes",
		));
	}
	wire::validate_frame_version(header.version)?;
	if header.flags & !FRAME_FLAG_KNOWN_MASK != 0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"HRFT frame carries unknown flag bits 0x{:04x}",
				header.flags & !FRAME_FLAG_KNOWN_MASK
			),
		));
	}
	check_section_len("meta", header.meta_len)?;
	check_section_len("arrow", header.arrow_len)?;
	check_section_len("trailing", header.trailing_len)?;
	Ok(*header)
}

/// Reads the variable-size frame sections after `header` has
/// already been validated by [`read_frame_header`].
pub async fn read_frame_from_header<R: AsyncRead + Unpin + ?Sized>(
	reader: &mut R,
	pool: &Arc<BufPool>,
	header: FrameHeader,
) -> std::io::Result<HrftFrame> {
	let meta = read_section(reader, pool, header.meta_len).await?.expect(
		"meta section always present (read_section returns Some for any len, \
         including zero, when the section is required)",
	);
	let arrow_payload = if header.arrow_len > 0 {
		read_section(reader, pool, header.arrow_len).await?
	} else {
		None
	};
	let trailing = if header.trailing_len > 0 {
		read_section(reader, pool, header.trailing_len).await?
	} else {
		None
	};

	Ok(HrftFrame {
		header,
		meta,
		arrow_payload,
		trailing,
	})
}

/// Writes an HRFT frame to an asynchronous stream. Takes borrowed
/// byte slices so callers can pass `&frame.meta` (derefs) or any
/// other `AsRef<[u8]>` without owning the buffers.
pub async fn write_frame<W: AsyncWrite + Unpin + ?Sized>(
	writer: &mut W,
	header: &FrameHeader,
	meta: &[u8],
	arrow_payload: Option<&[u8]>,
	trailing: Option<&[u8]>,
) -> std::io::Result<()> {
	writer.write_all(bytemuck::bytes_of(header)).await?;
	writer.write_all(meta).await?;
	if let Some(arrow) = arrow_payload {
		writer.write_all(arrow).await?;
	}
	if let Some(t) = trailing {
		writer.write_all(t).await?;
	}
	Ok(())
}

// ============================================================
//  INTERNAL HELPERS
// ============================================================

async fn read_section<R: AsyncRead + Unpin + ?Sized>(
	reader: &mut R,
	pool: &Arc<BufPool>,
	len: u32,
) -> std::io::Result<Option<PooledBuf>> {
	let mut buf = pool.acquire_at_least(len as usize);
	{
		let vec = buf.as_mut_vec();
		vec.clear();
		if len > 0 {
			vec.reserve(len as usize);
			let n = (&mut *reader).take(len as u64).read_to_end(vec).await?;
			if n != len as usize {
				return Err(Error::new(ErrorKind::UnexpectedEof, "early eof"));
			}
		}
	}
	Ok(Some(buf))
}

fn check_section_len(name: &'static str, len: u32) -> std::io::Result<()> {
	if len > MAX_SECTION_BYTES {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("HRFT {name} section exceeds {MAX_SECTION_BYTES}-byte safety cap (got {len})"),
		));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::schema::{FRAME_FLAG_STREAM_END, HRFT_ENVELOPE_VERSION};
	use tokio::io::AsyncWriteExt;

	#[tokio::test]
	async fn read_frame_header_rejects_unknown_flag_bits() {
		let header = FrameHeader {
			magic: HRFT_MAGIC,
			version: HRFT_ENVELOPE_VERSION,
			msg_type: 0,
			flags: FRAME_FLAG_STREAM_END | (1 << 15),
			stream_id: 7,
			meta_len: 0,
			arrow_len: 0,
			trailing_len: 0,
		};
		let (mut tx, mut rx) = tokio::io::duplex(64);
		tx.write_all(bytemuck::bytes_of(&header)).await.unwrap();
		drop(tx);

		let err = read_frame_header(&mut rx).await.unwrap_err();

		assert_eq!(err.kind(), ErrorKind::InvalidData);
		assert!(
			err.to_string()
				.contains("HRFT frame carries unknown flag bits 0x8000")
		);
	}
}
