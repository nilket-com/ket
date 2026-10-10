//! Private bounded content loader; synthetic capability construction only under cfg(test).
#![allow(dead_code)]
use super::*;
use crate::storage::mutation_visibility::{
	ProofKeyProvider,
	adapters::ProofSchema,
	provenance::{ContentVerifiedFrameProvenance, TrustedFrameRows},
};
use arrow::{
	ipc::{MessageHeader, Type, root_as_message},
	record_batch::RecordBatch,
};
use std::io::Cursor;

/// Unconstructable by ordinary runtime code until scoped authorization is reviewed.
pub(in crate::storage::mutation_visibility) struct LoaderCapability2 {
	scope: MutationIdentity,
	schema: Option<ProofSchema>,
	table_rows: bool,
	cluster: [u8; 16],
	claims: Vec<(MutationIdentity, Digest2, Log2)>,
}
impl LoaderCapability2 {
	#[cfg(test)]
	pub(super) fn synthetic(
		data: &UnverifiedData2,
		cluster: [u8; 16],
		schema: Option<ProofSchema>,
		table_rows: bool,
	) -> Self {
		let scope = data.bundle.claims[0].0.manifest.identity;
		let claims = data
			.bundle
			.claims
			.iter()
			.map(|(c, l)| {
				(
					c.manifest.identity,
					c.digest().expect("synthetic claim"),
					*l,
				)
			})
			.collect();
		Self {
			scope,
			schema,
			table_rows,
			cluster,
			claims,
		}
	}
}
pub(in crate::storage::mutation_visibility) struct LoadedContent2 {
	provenance: ContentVerifiedFrameProvenance,
}
impl std::fmt::Debug for LoadedContent2 {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("LoadedContent2 { content only; redacted }")
	}
}
// Parse all flatbuffer metadata and body extents before allocating any Arrow arrays.
pub(super) fn ipc_preflight(bytes: &[u8]) -> Result2<usize> {
	ipc_preflight_with(bytes, |_, _| {})
}
fn ipc_preflight_with(bytes: &[u8], mut boundary: impl FnMut(usize, usize)) -> Result2<usize> {
	if bytes.is_empty() || bytes.len() > FRAME_MAX {
		return Err(RootError2::Limit);
	}
	let mut pos = 0usize;
	let mut fields = 0;
	let mut rows = 0;
	let mut schema_seen = false;
	let mut batch_seen = false;
	let mut ended = false;
	let mut decoded_bytes = 0usize;
	while pos < bytes.len() {
		let message_start = pos;
		let take = |at: usize, n: usize| -> Result2<&[u8]> {
			bytes
				.get(at..at.checked_add(n).ok_or(RootError2::Overflow)?)
				.ok_or(RootError2::Codec)
		};
		let marker = u32::from_le_bytes(take(pos, 4)?.try_into().map_err(|_| RootError2::Codec)?);
		pos += 4;
		let len = if marker == u32::MAX {
			let n = u32::from_le_bytes(take(pos, 4)?.try_into().map_err(|_| RootError2::Codec)?);
			pos += 4;
			n
		} else {
			marker
		} as usize;
		if len == 0 {
			check(pos == bytes.len())?;
			ended = true;
			boundary(message_start, pos);
			break;
		}
		if len > 65536 {
			return Err(RootError2::Limit);
		}
		// Arrow consumes exactly metadata length and bodyLength, with no implicit padding.
		check(len % 8 == 0)?;
		let message = root_as_message(take(pos, len)?).map_err(|_| RootError2::Codec)?;
		pos = pos.checked_add(len).ok_or(RootError2::Overflow)?;
		check(message.custom_metadata().is_none_or(|m| m.is_empty()))?;
		let body_len = usize::try_from(message.bodyLength()).map_err(|_| RootError2::Codec)?;
		check(body_len % 8 == 0)?;
		let body = take(pos, body_len)?;
		match message.header_type() {
			MessageHeader::Schema => {
				check(!schema_seen && !batch_seen && body_len == 0)?;
				schema_seen = true;
				let schema = message.header_as_schema().ok_or(RootError2::Codec)?;
				check(schema.endianness() == arrow::ipc::Endianness::Little)?;
				check(schema.custom_metadata().is_none_or(|m| m.is_empty()))?;
				let fs = schema.fields().ok_or(RootError2::Codec)?;
				fields = fs.len();
				if fields == 0 || fields > 128 {
					return Err(RootError2::Limit);
				}
				for f in fs {
					check(
						f.dictionary().is_none()
							&& f.children().is_none_or(|x| x.is_empty())
							&& f.custom_metadata().is_none_or(|x| x.is_empty()),
					)?;
					let name = f.name().ok_or(RootError2::Codec)?;
					check(!name.is_empty() && name.len() <= 255 && !name.contains('\0'))?;
					if let Some(t) = f.type_as_int() {
						check(matches!(t.bitWidth(), 8 | 16 | 32 | 64))?;
					}
					if let Some(t) = f.type_as_fixed_size_binary() {
						check(t.byteWidth() == 16)?;
					}
					if let Some(t) = f.type_as_decimal() {
						check(t.bitWidth() == 128 && t.precision() > 0 && t.precision() <= 38)?;
					}
					if let Some(t) = f.type_as_floating_point() {
						check(matches!(
							t.precision(),
							arrow::ipc::Precision::SINGLE | arrow::ipc::Precision::DOUBLE
						))?;
					}
					check(matches!(
						f.type_type(),
						Type::Int
							| Type::FloatingPoint | Type::Bool
							| Type::Binary | Type::Utf8
							| Type::FixedSizeBinary
							| Type::Decimal | Type::Timestamp
							| Type::Date
					))?;
				}
			}
			MessageHeader::RecordBatch => {
				check(schema_seen && !batch_seen)?;
				batch_seen = true;
				let b = message.header_as_record_batch().ok_or(RootError2::Codec)?;
				check(
					b.compression().is_none()
						&& b.variadicBufferCounts().is_none_or(|x| x.is_empty()),
				)?;
				rows = usize::try_from(b.length()).map_err(|_| RootError2::Codec)?;
				if rows == 0 || rows > 512 {
					return Err(RootError2::Limit);
				}
				let nodes = b.nodes().ok_or(RootError2::Codec)?;
				check(nodes.len() == fields)?;
				for node in nodes {
					check(
						node.length() == rows as i64
							&& node.null_count() >= 0
							&& node.null_count() <= node.length(),
					)?;
				}
				let buffers = b.buffers().ok_or(RootError2::Codec)?;
				if buffers.len() > fields * 3 {
					return Err(RootError2::Limit);
				}
				let mut previous = 0usize;
				for buffer in buffers {
					let offset = usize::try_from(buffer.offset()).map_err(|_| RootError2::Codec)?;
					let length = usize::try_from(buffer.length()).map_err(|_| RootError2::Codec)?;
					let end = offset.checked_add(length).ok_or(RootError2::Overflow)?;
					check(offset >= previous && end <= body.len())?;
					previous = end;
					decoded_bytes = decoded_bytes
						.checked_add(length)
						.ok_or(RootError2::Overflow)?;
					if decoded_bytes > FRAME_MAX {
						return Err(RootError2::Limit);
					}
				}
			}
			_ => return Err(RootError2::Codec),
		}
		pos = pos.checked_add(body_len).ok_or(RootError2::Overflow)?;
		check(pos <= bytes.len())?;
		boundary(message_start, pos);
	}
	check(schema_seen && batch_seen && ended)?;
	Ok(rows)
}
pub(in crate::storage::mutation_visibility) fn decode_batch(bytes: &[u8]) -> Result2<RecordBatch> {
	let rows = ipc_preflight(bytes)?;
	#[cfg(test)]
	READER_CONSTRUCTIONS.with(|n| n.set(n.get() + 1));
	let mut reader = arrow::ipc::reader::StreamReader::try_new(Cursor::new(bytes), None)
		.map_err(|_| RootError2::Codec)?;
	let batch = reader
		.next()
		.ok_or(RootError2::Codec)?
		.map_err(|_| RootError2::Codec)?;
	check(batch.num_rows() == rows && reader.next().is_none())?;
	Ok(batch)
}
#[cfg(test)]
thread_local! { static READER_CONSTRUCTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) }; }
fn claim_mac(
	c: &ClaimProjection2,
	cluster: [u8; 16],
	provider: &impl ProofKeyProvider,
) -> Result2<Digest2> {
	let mut unsigned = c.clone();
	unsigned.claim_commitment = [0; 32];
	unsigned.signature = [0; 64];
	let encoded = unsigned.encode()?;
	let mut preimage = b"VIS-CLAIM-COMMIT-2\0".to_vec();
	preimage.extend(c.manifest.identity.generation);
	preimage.extend(cluster);
	let binding = c.manifest.binding();
	preimage.extend(binding.encode().map_err(|_| RootError2::Codec)?);
	blob(&mut preimage, &encoded)?;
	provider
		.keyed_digest(&binding, &preimage)
		.map_err(|_| RootError2::TrustMismatch)
}
pub(super) fn load_content2(
	data: UnverifiedData2,
	cap: &LoaderCapability2,
	provider: &impl ProofKeyProvider,
) -> Result2<LoadedContent2> {
	data.bundle.validate()?;
	data.bundle
		.provenance
		.check_companion(&data.frame)
		.map_err(|_| RootError2::Codec)?;
	nonzero16(cap.cluster)?;
	let scope = cap.scope.encode().map_err(|_| RootError2::Codec)?;
	check(data.bundle.claims.len() == cap.claims.len())?;
	for ((c, l), expected) in data.bundle.claims.iter().zip(&cap.claims) {
		check(
			c.manifest
				.identity
				.encode()
				.map_err(|_| RootError2::Codec)?[..49]
				== scope[..49]
				&& (c.manifest.identity, c.digest()?, *l) == *expected,
		)?;
		check(claim_mac(c, cap.cluster, provider)? == c.claim_commitment)?;
	}
	let rows =
		TrustedFrameRows::from_bounded_rows(cap, data.frame, cap.schema.clone(), cap.table_rows)
			.map_err(|_| RootError2::Codec)?;
	let provenance = data
		.bundle
		.provenance
		.verify(provider, &rows)
		.map_err(|_| RootError2::TrustMismatch)?;
	// Content verifies B.1 original versions and commitments. Signatures, grant application,
	// durable receipts and control acceptance are deliberately not promoted by this loader.
	Ok(LoadedContent2 { provenance })
}

#[cfg(test)]
#[path = "loader_tests.rs"]
pub(super) mod tests;
