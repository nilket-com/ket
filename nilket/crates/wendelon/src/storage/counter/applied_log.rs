//! The applied log's frame codec, its recovered image, and the identity checks
//! that bind applied records to pending witnesses.
//!
//! A frame is a little-endian `u32` payload length followed by the payload.
//! Two payload shapes exist:
//!
//! - **Keyed** (every frame this build appends): the two bytes `[0x00, 0x02]`,
//!   then postcard `{op_id, write_key, key, previous, current}`.
//! - **Unkeyed** (the original record, still read): postcard
//!   `{op_id, write_key, previous, current}`. Its first byte is the varint of a
//!   non-zero `op_id`, so it is never `0x00` and the shapes cannot be confused.
//!
//! An unkeyed record carries no counter key, so recovery can bind it to a
//! witness only by `op_id`, write key and outcome. That is the compatibility
//! limit of the older record; a keyed record must also match the witness's
//! counter key.
use std::collections::HashMap;
use std::io::{Error, ErrorKind};

use crate::storage::counter::witness::StoredWitness;
use crate::storage::counter::{
	AppliedCounterAdd, CounterKey, CounterVisibilityError, CounterWriteKey,
};

const KEYED_FRAME_TAG: [u8; 2] = [0x00, 0x02];

#[derive(serde::Serialize, serde::Deserialize)]
struct UnkeyedAppliedFrame {
	op_id: u64,
	write_key: CounterWriteKey,
	previous: u64,
	current: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct KeyedAppliedFrame {
	op_id: u64,
	write_key: CounterWriteKey,
	key: CounterKey,
	previous: u64,
	current: u64,
}

/// The applied log cannot take an append under its owner's authority. The store
/// returns this and leaves every file as it is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AppliedLogFault {
	/// The file's length is not the length this store last wrote. The node owner
	/// lock makes this store the log's only writer, so the difference is a
	/// foreign writer or corruption, never a reason to re-read.
	Diverged {
		expected_len: u64,
		observed_len: Option<u64>,
	},
	/// The store's durable state can no longer be described (an applied append
	/// or barrier failed, or a failed prepare could not withdraw its witness).
	/// Nothing further is published or appended until recovery reopens it.
	Unsettled,
	/// One write key names two operations: the dedup identity would map to two
	/// outcomes. Both representations are kept for repair.
	IdentityConflict {
		write_key: CounterWriteKey,
		recorded_op_id: u64,
		conflicting_op_id: u64,
	},
}

impl std::fmt::Display for AppliedLogFault {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "counter applied log {self:?}")
	}
}
impl std::error::Error for AppliedLogFault {}
impl AppliedLogFault {
	pub(crate) fn io(self) -> Error {
		Error::new(ErrorKind::InvalidData, self)
	}
}

/// An incomplete final frame: the bytes from `valid_len` on are a torn append.
#[derive(Clone, Debug)]
pub(super) struct TornAppliedTail {
	pub(super) valid_len: u64,
	/// The torn frame's operation id when enough of it survived to decode.
	pub(super) op_id: Option<u64>,
	/// The torn bytes themselves, so a repair can require them to be a strict
	/// prefix of the frame the owning operation would have written.
	pub(super) bytes: Vec<u8>,
}

fn identity_conflict(
	write_key: CounterWriteKey,
	recorded_op_id: u64,
	conflicting_op_id: u64,
) -> Error {
	AppliedLogFault::IdentityConflict {
		write_key,
		recorded_op_id,
		conflicting_op_id,
	}
	.io()
}

/// The parsed log plus the length the owning store expects on disk.
pub(super) struct AppliedLogImage {
	pub(super) records: Vec<AppliedCounterAdd>,
	by_op: HashMap<u64, usize>,
	by_key: HashMap<CounterWriteKey, usize>,
	pub(super) torn_tail: Option<TornAppliedTail>,
	pub(super) file_len: u64,
	/// False after this store created the file until its directory entry is synced.
	pub(super) name_durable: bool,
}

impl AppliedLogImage {
	fn new(
		records: Vec<AppliedCounterAdd>,
		torn_tail: Option<TornAppliedTail>,
		file_len: u64,
	) -> std::io::Result<Self> {
		let mut image = Self {
			records: Vec::with_capacity(records.len()),
			by_op: HashMap::new(),
			by_key: HashMap::new(),
			torn_tail,
			file_len,
			name_durable: true,
		};
		for record in records {
			if record.op_id == 0 || record.current <= record.previous {
				return Err(CounterVisibilityError::Conflict.io());
			}
			let same_op = image.by_op.get(&record.op_id).copied();
			let same_key = image.by_key.get(&record.write_key).copied();
			if let Some(index) = same_key {
				let recorded = image.records[index].op_id;
				if recorded != record.op_id {
					return Err(identity_conflict(record.write_key, recorded, record.op_id));
				}
			}
			for index in [same_op, same_key].into_iter().flatten() {
				if !same_applied(&image.records[index], &record) {
					return Err(CounterVisibilityError::Conflict.io());
				}
			}
			// An identical duplicate (a retried resolution that crashed before its
			// witness was pruned) is one logical record: keep its first frame.
			if same_op.is_none() {
				image.insert(record);
			}
		}
		Ok(image)
	}

	fn insert(&mut self, record: AppliedCounterAdd) {
		let index = self.records.len();
		self.by_op.insert(record.op_id, index);
		self.by_key.insert(record.write_key, index);
		self.records.push(record);
	}

	pub(super) fn by_op(&self, op_id: u64) -> Option<&AppliedCounterAdd> {
		self.by_op.get(&op_id).map(|index| &self.records[*index])
	}

	pub(super) fn by_key(&self, write_key: &CounterWriteKey) -> Option<&AppliedCounterAdd> {
		self.by_key
			.get(write_key)
			.map(|index| &self.records[*index])
	}

	/// Record a frame this store appended and made durable.
	pub(super) fn push(&mut self, record: AppliedCounterAdd, frame_len: usize) {
		self.insert(record);
		self.file_len = self.file_len.saturating_add(frame_len as u64);
	}
}

/// Encode `applied` as a keyed frame (length prefix included).
pub(super) fn encode_frame(applied: &AppliedCounterAdd) -> std::io::Result<Vec<u8>> {
	let key = applied
		.key
		.clone()
		.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "applied frame needs its key"))?;
	let body = postcard::to_allocvec(&KeyedAppliedFrame {
		op_id: applied.op_id,
		write_key: applied.write_key,
		key,
		previous: applied.previous,
		current: applied.current,
	})
	.map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
	frame(&KEYED_FRAME_TAG, &body)
}

fn frame(tag: &[u8], body: &[u8]) -> std::io::Result<Vec<u8>> {
	let len = u32::try_from(tag.len() + body.len())
		.map_err(|_| Error::new(ErrorKind::InvalidData, "applied frame too large"))?;
	let mut frame = Vec::with_capacity(4 + tag.len() + body.len());
	frame.extend_from_slice(&len.to_le_bytes());
	frame.extend_from_slice(tag);
	frame.extend_from_slice(body);
	Ok(frame)
}

/// Encode an unkeyed frame exactly as the original writer did.
pub(super) fn encode_unkeyed_frame(applied: &AppliedCounterAdd) -> std::io::Result<Vec<u8>> {
	let body = postcard::to_allocvec(&UnkeyedAppliedFrame {
		op_id: applied.op_id,
		write_key: applied.write_key,
		previous: applied.previous,
		current: applied.current,
	})
	.map_err(|error| Error::new(ErrorKind::InvalidData, error))?;
	frame(&[], &body)
}

/// How many leading bytes of `frame` identify its operation: the length
/// prefix, the shape tag and the complete `op_id` varint.
#[cfg(test)]
pub(super) fn identifying_prefix_len(frame: &[u8]) -> usize {
	let payload = &frame[4..];
	let tag = if payload.first() == Some(&0) {
		KEYED_FRAME_TAG.len()
	} else {
		0
	};
	let (_, rest) = postcard::take_from_bytes::<u64>(&payload[tag..]).unwrap();
	4 + tag + (payload.len() - tag - rest.len())
}

fn decode_payload(payload: &[u8]) -> std::io::Result<AppliedCounterAdd> {
	let invalid = |error: postcard::Error| Error::new(ErrorKind::InvalidData, error);
	let trailing = || Error::new(ErrorKind::InvalidData, "applied frame has trailing bytes");
	if payload.first() == Some(&0) {
		let body = payload
			.strip_prefix(&KEYED_FRAME_TAG[..])
			.ok_or_else(|| Error::new(ErrorKind::InvalidData, "unknown applied frame shape"))?;
		let (frame, rest) =
			postcard::take_from_bytes::<KeyedAppliedFrame>(body).map_err(invalid)?;
		if !rest.is_empty() {
			return Err(trailing());
		}
		return Ok(AppliedCounterAdd {
			op_id: frame.op_id,
			write_key: frame.write_key,
			key: Some(frame.key),
			previous: frame.previous,
			current: frame.current,
		});
	}
	let (frame, rest) =
		postcard::take_from_bytes::<UnkeyedAppliedFrame>(payload).map_err(invalid)?;
	if !rest.is_empty() {
		return Err(trailing());
	}
	Ok(AppliedCounterAdd {
		op_id: frame.op_id,
		write_key: frame.write_key,
		key: None,
		previous: frame.previous,
		current: frame.current,
	})
}

/// The operation id of a torn payload. `None` only while the payload is still
/// shorter than its shape tag plus a complete `op_id` varint; a wrong tag or an
/// overlong or overflowing varint is corruption, never a torn append.
fn torn_op_id(partial: &[u8]) -> std::io::Result<Option<u64>> {
	let corrupt = |detail: &str| Error::new(ErrorKind::InvalidData, detail.to_owned());
	let body = match partial {
		[] | [0] => return Ok(None),
		[0, tag, body @ ..] if *tag == KEYED_FRAME_TAG[1] => body,
		[0, ..] => return Err(corrupt("unknown applied frame shape in torn tail")),
		body => body,
	};
	const MAX_VARINT_LEN: usize = 10;
	if !body
		.iter()
		.take(MAX_VARINT_LEN)
		.any(|byte| byte & 0x80 == 0)
	{
		return if body.len() < MAX_VARINT_LEN {
			Ok(None)
		} else {
			Err(corrupt("overlong operation id in torn tail"))
		};
	}
	postcard::take_from_bytes::<u64>(body)
		.map(|(op_id, _)| Some(op_id))
		.map_err(|error| Error::new(ErrorKind::InvalidData, error))
}

/// Parse a whole log. A complete but undecodable frame is an error; only an
/// incomplete final frame is reported as a torn tail.
pub(super) fn parse(bytes: &[u8]) -> std::io::Result<AppliedLogImage> {
	let mut records = Vec::new();
	let mut at = 0usize;
	while at < bytes.len() {
		let rest = &bytes[at..];
		let torn = |op_id| {
			Some(TornAppliedTail {
				valid_len: at as u64,
				op_id,
				bytes: rest.to_vec(),
			})
		};
		if rest.len() < 4 {
			return AppliedLogImage::new(records, torn(None), bytes.len() as u64);
		}
		let len = u32::from_le_bytes(rest[..4].try_into().expect("four-byte prefix")) as usize;
		if len == 0 {
			return Err(CounterVisibilityError::Conflict.io());
		}
		let payload = &rest[4..];
		if len > payload.len() {
			return AppliedLogImage::new(records, torn(torn_op_id(payload)?), bytes.len() as u64);
		}
		records.push(decode_payload(&payload[..len])?);
		at += 4 + len;
	}
	AppliedLogImage::new(records, None, bytes.len() as u64)
}

/// Two records of one logical operation: every field both carry is equal.
pub(super) fn same_applied(a: &AppliedCounterAdd, b: &AppliedCounterAdd) -> bool {
	a.op_id == b.op_id
		&& a.write_key == b.write_key
		&& a.previous == b.previous
		&& a.current == b.current
		&& match (&a.key, &b.key) {
			(Some(a), Some(b)) => a == b,
			_ => true,
		}
}

/// A witness and an applied record describe the same operation: every field
/// both carry is equal (an unkeyed record cannot be checked against the key).
pub(super) fn witness_matches_applied(
	witness: &StoredWitness,
	applied: &AppliedCounterAdd,
) -> bool {
	witness.op_id == applied.op_id
		&& witness.write_key == applied.write_key
		&& witness.previous == applied.previous
		&& witness.value == applied.current
		&& applied.key.as_ref().is_none_or(|key| *key == witness.key)
}

pub(super) fn validate_witness_set(witnesses: &[StoredWitness]) -> std::io::Result<()> {
	let mut by_op = HashMap::new();
	let mut by_key = HashMap::new();
	for witness in witnesses {
		if let Some(old) = by_op.insert(witness.op_id, witness) {
			if old.write_key != witness.write_key
				|| old.previous != witness.previous
				|| old.value != witness.value
				|| old.key != witness.key
			{
				return Err(CounterVisibilityError::Conflict.io());
			}
		}
		if let Some(old) = by_key.insert(witness.write_key, witness) {
			if old.op_id != witness.op_id {
				return Err(identity_conflict(
					witness.write_key,
					old.op_id,
					witness.op_id,
				));
			}
			if old.previous != witness.previous
				|| old.value != witness.value
				|| old.key != witness.key
			{
				return Err(CounterVisibilityError::Conflict.io());
			}
		}
	}
	Ok(())
}

/// Every pending witness that shares an operation id or write key with an
/// applied record must describe the same operation.
pub(super) fn validate_pairs(
	image: &AppliedLogImage,
	witnesses: &[StoredWitness],
) -> std::io::Result<()> {
	for witness in witnesses {
		if let Some(record) = image.by_key(&witness.write_key) {
			if record.op_id != witness.op_id {
				return Err(identity_conflict(
					witness.write_key,
					record.op_id,
					witness.op_id,
				));
			}
		}
		let records = [image.by_op(witness.op_id), image.by_key(&witness.write_key)];
		if records
			.into_iter()
			.flatten()
			.any(|record| !witness_matches_applied(witness, record))
		{
			return Err(CounterVisibilityError::Conflict.io());
		}
	}
	Ok(())
}

/// The record a resolved witness compacts into.
pub(super) fn record_for(witness: &StoredWitness) -> AppliedCounterAdd {
	AppliedCounterAdd {
		op_id: witness.op_id,
		write_key: witness.write_key,
		key: Some(witness.key.clone()),
		previous: witness.previous,
		current: witness.value,
	}
}

/// Whether a torn tail is a strict prefix of the frame `record` would have
/// produced (keyed, or unkeyed as an older writer appended it), naming the same
/// operation when it names one. This bounds the tail by length and content: an
/// oversized length prefix in front of later bytes can never pass.
pub(super) fn tail_is_prefix_of(tail: &TornAppliedTail, record: &AppliedCounterAdd) -> bool {
	let strict_prefix =
		|frame: &[u8]| tail.bytes.len() < frame.len() && frame.starts_with(&tail.bytes);
	tail.op_id.is_none_or(|op_id| op_id == record.op_id)
		&& !tail.bytes.is_empty()
		&& (encode_frame(record).is_ok_and(|frame| strict_prefix(&frame))
			|| encode_unkeyed_frame(record).is_ok_and(|frame| strict_prefix(&frame)))
}

/// A torn tail may be truncated only while a durable witness still represents
/// the operation it belonged to. Appends are serialized and each is synced
/// before the next, so the torn frame is the last append, whose witness is
/// unlinked only after that append's barrier. The tail must be a strict
/// prefix of the frame some unresolved pending witness would have written.
pub(super) fn check_torn_tail_owner(
	image: &AppliedLogImage,
	tail: &TornAppliedTail,
	witnesses: &[StoredWitness],
) -> std::io::Result<()> {
	let owned = witnesses.iter().any(|witness| {
		image.by_op(witness.op_id).is_none() && tail_is_prefix_of(tail, &record_for(witness))
	});
	if owned {
		Ok(())
	} else {
		Err(CounterVisibilityError::Conflict.io())
	}
}
