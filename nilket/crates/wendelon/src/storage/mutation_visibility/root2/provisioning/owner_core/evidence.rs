//! Repair-evidence codec: the integrity-bound context, evidence file names and the byte layout.
use super::*;

const MAGIC: &[u8; 8] = b"VISREP03";
const VERSION: u16 = 1;
const CONTEXT_DOMAIN: &[u8] = b"VISROOT2-REPAIR-CONTEXT\0";
const OP_DOMAIN: &[u8] = b"VISROOT2-REPAIR-OP\0";

/// Integrity binding of a repair to its validated prefix, boundary and torn tail.
pub(in crate::storage::mutation_visibility::root2) fn repair_context2(
	prefix_sha256: &Digest2,
	boundary: u64,
	original_len: u64,
	tail_sha256: &Digest2,
) -> Digest2 {
	let mut body = Vec::with_capacity(80);
	body.extend_from_slice(prefix_sha256);
	body.extend_from_slice(&boundary.to_le_bytes());
	body.extend_from_slice(&original_len.to_le_bytes());
	body.extend_from_slice(tail_sha256);
	hash(CONTEXT_DOMAIN, &body)
}
pub(in crate::storage::mutation_visibility::root2) fn repair_op_id2(context: &Digest2) -> [u8; 16] {
	hash(OP_DOMAIN, context)[..16]
		.try_into()
		.expect("16 of 32 digest bytes")
}
pub(in crate::storage::mutation_visibility::root2) fn evidence_name2(context: &Digest2) -> String {
	const HEX: &[u8; 16] = b"0123456789abcdef";
	let mut name = String::with_capacity(68);
	for b in context {
		name.push(HEX[usize::from(b >> 4)] as char);
		name.push(HEX[usize::from(b & 15)] as char);
	}
	name.push_str(".rep");
	name
}

pub(in crate::storage::mutation_visibility::root2) fn expected_repair_event2(
	e: &RepairEvidence2,
) -> Result<[u8; EVENT_BYTES2], EvidenceError2> {
	let tail_len = u32::try_from(e.tail.len()).map_err(|_| EvidenceError2::Event)?;
	encode_event2(&Event2 {
		seq: e.repair_seq,
		op_id: e.repair_op_id,
		identity: None,
		body: EventBody2::Repair {
			tail_sha256: e.tail_sha256,
			original_len: e.original_len,
			boundary: e.boundary,
			tail_len,
		},
	})
	.map_err(|_| EvidenceError2::Event)
}

fn array<const N: usize>(b: &[u8], at: usize) -> [u8; N] {
	b[at..at + N].try_into().expect("fixed evidence offset")
}

/// Standalone checks only: no claim about the log prefix or prefix operation ids.
pub(in crate::storage::mutation_visibility::root2) fn decode_evidence2(
	name: &str,
	bytes: &[u8],
) -> Result<RepairEvidence2, EvidenceError2> {
	use EvidenceError2 as E;
	if !hex_name(name, ".rep") {
		return Err(E::Name);
	}
	// Length is settled before anything is sliced or copied.
	if bytes.len() < HEADER + 1 + TRAILER || bytes.len() > MAX_EVIDENCE_BYTES2 {
		return Err(E::Length);
	}
	if &bytes[0..8] != MAGIC {
		return Err(E::Magic);
	}
	if u16::from_le_bytes(array(bytes, 8)) != VERSION {
		return Err(E::Version);
	}
	if bytes[10..12] != [0; 2] || bytes[64..68] != [0; 4] {
		return Err(E::Reserved);
	}
	let tail_len = u32::from_le_bytes(array(bytes, 60)) as usize;
	if !(1..=MAX_TAIL2).contains(&tail_len) {
		return Err(E::TailLength);
	}
	if bytes.len() != HEADER + tail_len + TRAILER {
		return Err(E::Length);
	}
	let body = bytes.len() - TRAILER;
	if hash(b"", &bytes[..body]) != array::<32>(bytes, body) {
		return Err(E::Trailer);
	}
	let tail = &bytes[HEADER..body];
	let tail_sha256: Digest2 = array(bytes, 68);
	if hash(b"", tail) != tail_sha256 {
		return Err(E::TailHash);
	}
	let boundary = u64::from_le_bytes(array(bytes, 44));
	let original_len = u64::from_le_bytes(array(bytes, 52));
	if boundary % EVENT_BYTES2 as u64 != 0
		|| boundary.checked_add(tail_len as u64) != Some(original_len)
	{
		return Err(E::Bounds);
	}
	let repair_seq = u64::from_le_bytes(array(bytes, 116));
	if !(1..=TOTAL_EVENTS2).contains(&repair_seq)
		|| (repair_seq - 1).checked_mul(EVENT_BYTES2 as u64) != Some(boundary)
	{
		return Err(E::Sequence);
	}
	let prefix_sha256: Digest2 = array(bytes, 12);
	let context = repair_context2(&prefix_sha256, boundary, original_len, &tail_sha256);
	if name != evidence_name2(&context) {
		return Err(E::Context);
	}
	let repair_op_id: [u8; 16] = array(bytes, 100);
	if repair_op_id == [0; 16] || repair_op_id != repair_op_id2(&context) {
		return Err(E::OpId);
	}
	let evidence = RepairEvidence2 {
		prefix_sha256,
		boundary,
		original_len,
		tail_sha256,
		repair_op_id,
		repair_seq,
		tail: tail.to_vec(),
	};
	expected_repair_event2(&evidence)?;
	Ok(evidence)
}

/// Contextual creation for an Ambiguous log: binds the validated prefix, refuses a derived op id
/// already used in that prefix, and emits only bytes the decoder accepts.
pub(in crate::storage::mutation_visibility::root2) fn encode_evidence2(
	log: &[u8],
) -> Result<(String, Vec<u8>), EvidenceError2> {
	use EvidenceError2 as E;
	let Err(LogError2::Ambiguous {
		boundary,
		tail_len,
		tail_sha256,
	}) = fold_events2(log)
	else {
		return Err(E::NotAmbiguous);
	};
	let b = usize::try_from(boundary).map_err(|_| E::Bounds)?;
	let prefix_sha256 = hash(b"", &log[..b]);
	let original_len = u64::try_from(log.len()).map_err(|_| E::Bounds)?;
	let context = repair_context2(&prefix_sha256, boundary, original_len, &tail_sha256);
	let repair_op_id = repair_op_id2(&context);
	if repair_op_id == [0; 16] {
		return Err(E::OpId);
	}
	if prefix_has_op_id(&log[..b], &repair_op_id) {
		return Err(E::OpIdCollision);
	}
	let repair_seq = boundary / EVENT_BYTES2 as u64 + 1;
	if repair_seq > TOTAL_EVENTS2 {
		return Err(E::Sequence);
	}
	let mut bytes = Vec::with_capacity(HEADER + tail_len as usize + TRAILER);
	bytes.extend_from_slice(MAGIC);
	bytes.extend_from_slice(&VERSION.to_le_bytes());
	bytes.extend_from_slice(&[0; 2]);
	bytes.extend_from_slice(&prefix_sha256);
	bytes.extend_from_slice(&boundary.to_le_bytes());
	bytes.extend_from_slice(&original_len.to_le_bytes());
	bytes.extend_from_slice(&tail_len.to_le_bytes());
	bytes.extend_from_slice(&[0; 4]);
	bytes.extend_from_slice(&tail_sha256);
	bytes.extend_from_slice(&repair_op_id);
	bytes.extend_from_slice(&repair_seq.to_le_bytes());
	bytes.extend_from_slice(&log[b..]);
	let sum = hash(b"", &bytes);
	bytes.extend_from_slice(&sum);
	let name = evidence_name2(&context);
	decode_evidence2(&name, &bytes)?;
	Ok((name, bytes))
}
