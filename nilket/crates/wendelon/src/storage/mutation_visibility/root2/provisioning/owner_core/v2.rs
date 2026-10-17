//! Format-v2 owner decisions and evidence. The v1 implementation remains frozen in the parent.
use super::super::inventory::v2::{
	EVENT_BYTES2, MAX_TAIL2, decode_event2, encode_event2, fold_events2,
};
use super::*;
const MAGIC: &[u8; 8] = b"VISREP04";
const VERSION: u16 = 1;
const CONTEXT_DOMAIN: &[u8] = b"VISROOT2-REPAIR-CONTEXT-V2\0";
const OP_DOMAIN: &[u8] = b"VISROOT2-REPAIR-OP-V2\0";
pub(in crate::storage::mutation_visibility::root2::provisioning) const MAX_EVIDENCE_BYTES2: usize =
	HEADER + MAX_TAIL2 + TRAILER;
fn whole_events(log: &[u8]) -> impl Iterator<Item = Result<Event2, LogError2>> + '_ {
	log.chunks_exact(EVENT_BYTES2).map(decode_event2)
}
fn find_root<'a>(state: &'a LogState2, id: &RootIdentity2) -> Result<&'a RootEntry2, OwnerError2> {
	if let Some(root) = state.roots.iter().find(|r| r.identity == *id) {
		return Ok(root);
	}
	let same_key = state
		.roots
		.iter()
		.any(|r| (r.identity.generation, r.identity.cluster) == (id.generation, id.cluster));
	Err(if same_key {
		OwnerError2::IdentityMismatch
	} else {
		OwnerError2::NoSuchRoot
	})
}

/// The decision for one request against one complete log image. The state is derived from that
/// log only. A Retry is a decision, not a durability claim: the caller must re-sync first.
pub(in crate::storage::mutation_visibility::root2) fn decide2(
	log: &[u8],
	op_id: &OpId2,
	req: &OwnerRequest2,
) -> Decision2 {
	decide(log, op_id, req).unwrap_or_else(Decision2::Refuse)
}
fn decide(log: &[u8], op_id: &OpId2, req: &OwnerRequest2) -> Result<Decision2, OwnerError2> {
	let key = request_key2(req)?;
	let state = fold_events2(log).map_err(OwnerError2::Log)?;
	for event in whole_events(log) {
		let event = event.map_err(OwnerError2::Log)?;
		if event.op_id != op_id.0 {
			continue;
		}
		let reused = OwnerError2::OpIdReused { seq: event.seq };
		let Some((op, original)) = event_request_key2(&event) else {
			return Err(reused);
		};
		if original != key {
			return Err(reused);
		}
		let identity = event.identity.ok_or(OwnerError2::Inconsistent)?;
		let root = state
			.roots
			.iter()
			.find(|r| r.identity == identity)
			.ok_or(OwnerError2::Inconsistent)?;
		if let OwnerRequest2::Retire { terminal, .. } = req {
			if root.manifest.profile()
				== super::super::super::admin::manifest::ManifestProfile2::Three
			{
				return retired_retry(root, terminal.as_ref());
			}
			if terminal.is_some() {
				return Err(OwnerError2::ProfileUnsupported);
			}
		}
		return Ok(Decision2::Retry {
			original: EventRef2 { op, seq: event.seq },
			current: root.state,
		});
	}
	let next = state.last_seq.checked_add(1).ok_or(OwnerError2::LogFull)?;
	let ordinary = || {
		if next > ORDINARY_EVENTS2 {
			Err(OwnerError2::LogFull)
		} else {
			Ok(())
		}
	};
	let (identity, body) = match (req, &key) {
		(OwnerRequest2::Reserve { manifest }, RequestKey2::Reserve { manifest: bytes }) => {
			let identity = RootIdentity2 {
				generation: manifest.admission().generation,
				cluster: manifest.admission().cluster,
				pin: any_manifest_digest2(bytes),
				admin_key: manifest.admission().admin_key,
			};
			if state.roots.iter().any(|r| r.identity == identity) {
				return Err(OwnerError2::AlreadyReserved);
			}
			if state
				.roots
				.iter()
				.any(|r| shares_component(&r.identity, &identity))
			{
				return Err(OwnerError2::IdentityTaken);
			}
			ordinary()?;
			if state.roots.len() >= MAX_ROOTS2 {
				return Err(OwnerError2::RootCap);
			}
			(
				identity,
				EventBody2::Reserve {
					manifest: manifest.clone(),
				},
			)
		}
		(
			OwnerRequest2::Approve {
				identity,
				observation,
				..
			},
			_,
		) => {
			let root = find_root(&state, identity)?;
			let o = &observation.identity;
			match root.state {
				RootState2::Prepared => {
					ordinary()?;
					let binds = (o.generation, o.cluster, o.pin)
						== (identity.generation, identity.cluster, identity.pin)
						&& o.initial_epoch == root.manifest.admission().initial_epoch;
					let body = if binds {
						EventBody2::Approve {
							record: BootstrapRecord2 {
								identity: *o,
								inventory_seq: next,
							},
						}
					} else {
						EventBody2::Conflict {
							observation: *observation,
						}
					};
					(*identity, body)
				}
				RootState2::Bound => {
					return if root.record.map(|r| r.identity) == Some(*o) {
						Ok(Decision2::NoOp(RootState2::Bound))
					} else {
						Err(OwnerError2::BoundConflict)
					};
				}
				RootState2::Conflicted | RootState2::Retired => {
					return Err(OwnerError2::NotPrepared);
				}
			}
		}
		(OwnerRequest2::Retire { identity, terminal }, _) => {
			let root = find_root(&state, identity)?;
			if root.manifest.profile()
				== super::super::super::admin::manifest::ManifestProfile2::Three
			{
				if root.state == RootState2::Retired {
					return retired_retry(root, terminal.as_ref());
				}
				let o = terminal
					.as_ref()
					.ok_or(OwnerError2::CommittedRetirementRequired)?;
				if root.state != RootState2::Bound {
					return Err(OwnerError2::NotBound);
				}
				let record = root.record.ok_or(OwnerError2::Inconsistent)?;
				if let Some(field) = record.identity.first_mismatch(&o.identity) {
					return Err(OwnerError2::RetirementField(field));
				}
				if o.original_admin_key != root.identity.admin_key {
					return Err(OwnerError2::RetirementMismatch);
				}
				ordinary()?;
				let terminal = super::super::terminal::terminal_record3(o, next)
					.map_err(|_| OwnerError2::Codec)?;
				(*identity, EventBody2::RetireTerminal { terminal })
			} else {
				if terminal.is_some() {
					return Err(OwnerError2::ProfileUnsupported);
				}
				if root.state == RootState2::Retired {
					return Ok(Decision2::NoOp(RootState2::Retired));
				}
				ordinary()?;
				(*identity, EventBody2::Retire)
			}
		}
		_ => return Err(OwnerError2::Inconsistent),
	};
	let event = Event2 {
		seq: next,
		op_id: op_id.0,
		identity: Some(identity),
		body,
	};
	// Preflight: only an event the accepted fold admits as the next one is ever proposed.
	let bytes = encode_event2(&event).map_err(|_| OwnerError2::Codec)?;
	let mut extended = Vec::with_capacity(log.len() + EVENT_BYTES2);
	extended.extend_from_slice(log);
	extended.extend_from_slice(&bytes);
	match fold_events2(&extended) {
		Ok(s) if s.last_seq == next => Ok(Decision2::Append(event)),
		_ => Err(OwnerError2::Inconsistent),
	}
}

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
fn hex_name(name: &str, suffix: &str) -> bool {
	let b = name.as_bytes();
	b.len() == 64 + suffix.len()
		&& name.ends_with(suffix)
		&& b[..64]
			.iter()
			.all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'))
}
// The one collision check: an already-derived repair op id against the prefix's decoded event ids.
fn prefix_has_op_id(prefix: &[u8], op: &[u8; 16]) -> bool {
	whole_events(prefix).any(|e| e.is_ok_and(|e| e.op_id == *op))
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
	if bytes[10..12] != 2u16.to_le_bytes() {
		return Err(E::Format);
	}
	if bytes[64..68] != [0; 4] {
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
	bytes.extend_from_slice(&2u16.to_le_bytes());
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

// P2/P3: a tool-written Repair prefix, possibly followed by zero bytes the filesystem exposed.
fn partial_repair(tail: &[u8], expected: &[u8; EVENT_BYTES2]) -> bool {
	if tail.len() > EVENT_BYTES2 {
		return false;
	}
	let k = tail
		.iter()
		.zip(expected)
		.take_while(|(a, b)| a == b)
		.count();
	tail[k..].iter().all(|&x| x == 0)
}

/// Classifies a complete log image against the repairs directory's entries, in the fixed order:
/// entry count, names, evidence length bounds, log fold errors, evidence decoding, candidates.
pub(in crate::storage::mutation_visibility::root2) fn classify2(
	log: &[u8],
	entries: &[(&str, &[u8])],
) -> Classification2 {
	if entries.len() > MAX_REPAIR_ENTRIES2 {
		return Classification2::RepairDirFull;
	}
	let index = |i: usize| i as u32;
	for (i, (name, _)) in entries.iter().enumerate() {
		if !hex_name(name, ".rep") && !hex_name(name, ".rep.tmp") {
			return Classification2::RepairDirForeign { index: index(i) };
		}
	}
	for (i, (name, bytes)) in entries.iter().enumerate() {
		if hex_name(name, ".rep") && bytes.len() > MAX_EVIDENCE_BYTES2 {
			return Classification2::EvidenceInvalid {
				index: index(i),
				error: EvidenceError2::Length,
			};
		}
	}
	let (boundary, tail_info, last_seq) = match fold_events2(log) {
		Ok(state) => (log.len(), None, state.last_seq),
		Err(LogError2::Ambiguous {
			boundary,
			tail_len,
			tail_sha256,
		}) => (boundary as usize, Some((tail_len, tail_sha256)), 0),
		Err(e) => return Classification2::Log(e),
	};
	let mut evidence = Vec::new();
	for (i, (name, bytes)) in entries.iter().enumerate() {
		if hex_name(name, ".rep") {
			match decode_evidence2(name, bytes) {
				Ok(e) => evidence.push(e),
				Err(error) => {
					return Classification2::EvidenceInvalid {
						index: index(i),
						error,
					};
				}
			}
		}
	}
	let prefix = &log[..boundary];
	let tail = &log[boundary..];
	let prefix_sha256 = hash(b"", prefix);
	let mut found: Vec<(Digest2, RepairStage2, [u8; 16])> = Vec::new();
	for e in &evidence {
		if e.boundary != boundary as u64 || e.prefix_sha256 != prefix_sha256 {
			continue;
		}
		let stage = if tail.is_empty() {
			RepairStage2::Truncated
		} else if e.tail.starts_with(tail) {
			RepairStage2::NotTruncated
		} else if expected_repair_event2(e).is_ok_and(|x| partial_repair(tail, &x)) {
			RepairStage2::PartialRepair
		} else {
			continue;
		};
		let context = repair_context2(&e.prefix_sha256, e.boundary, e.original_len, &e.tail_sha256);
		if !found.iter().any(|(c, ..)| *c == context) {
			found.push((context, stage, e.repair_op_id));
		}
	}
	for (context, _, op) in &found {
		if prefix_has_op_id(prefix, op) {
			return Classification2::RepairOpIdCollision { context: *context };
		}
	}
	match (found.as_slice(), tail_info) {
		([], None) => Classification2::Clean { last_seq },
		([], Some((tail_len, tail_sha256))) => Classification2::Unclaimed {
			boundary: boundary as u64,
			tail_len,
			tail_sha256,
			prefix_sha256,
		},
		([(context, stage, _)], _) => Classification2::Pending {
			context: *context,
			stage: *stage,
		},
		(many, _) => Classification2::Conflicting {
			count: many.len() as u32,
		},
	}
}

fn retired_retry(
	root: &RootEntry2,
	observation: Option<&super::super::terminal::TerminalObservation3>,
) -> Result<Decision2, OwnerError2> {
	let Some(o) = observation else {
		return Err(if root.terminal.is_some() {
			OwnerError2::RetirementMismatch
		} else {
			OwnerError2::CommittedRetirementRequired
		});
	};
	let terminal = root.terminal.ok_or(OwnerError2::RetirementMismatch)?;
	let digest = super::super::terminal::observation_digest3(o)
		.map_err(|_| OwnerError2::RetirementMismatch)?;
	if digest != terminal.observation_digest {
		return Err(OwnerError2::RetirementMismatch);
	}
	Ok(Decision2::NoOp(RootState2::Retired))
}
