//! Directory-v2 fixed events (VISOEV02), the only inventory codec. Public record data only; no
//! trust or I/O.
use super::*;
pub(in crate::storage::mutation_visibility::root2::provisioning) const EVENT_BYTES2: usize = 512;
pub(in crate::storage::mutation_visibility::root2::provisioning) const MAX_TAIL2: usize = 1023;
pub(in crate::storage::mutation_visibility::root2::provisioning) const MAX_LOG_BYTES2: usize =
	TOTAL_EVENTS2 as usize * EVENT_BYTES2 + EVENT_BYTES2 - 1;
const MAGIC_V2: &[u8; 8] = b"VISOEV02";

pub(in crate::storage::mutation_visibility::root2::provisioning) fn encode_event2(
	e: &Event2,
) -> Result<[u8; EVENT_BYTES2], LogError2> {
	let bad = |r| corrupt(e.seq, r);
	let mut b = [0; EVENT_BYTES2];
	b[..8].copy_from_slice(MAGIC_V2);
	b[8..10].copy_from_slice(&1u16.to_le_bytes());
	b[10] = match e.body {
		EventBody2::Reserve { .. } => RESERVE,
		EventBody2::Approve { .. } => APPROVE,
		EventBody2::Conflict { .. } => CONFLICT,
		EventBody2::RetireTerminal { .. } => RETIRE,
		EventBody2::Repair { .. } => REPAIR,
	};
	b[12..20].copy_from_slice(&e.seq.to_le_bytes());
	b[20..36].copy_from_slice(&e.op_id);
	if let Some(i) = e.identity {
		b[36..52].copy_from_slice(&i.generation);
		b[52..68].copy_from_slice(&i.cluster);
		b[68..100].copy_from_slice(&i.pin);
		b[100..132].copy_from_slice(&i.admin_key);
	}
	let payload = match &e.body {
		EventBody2::Reserve { manifest } => manifest
			.encode()
			.map_err(|_| bad(CorruptReason2::Payload))?
			.as_slice()
			.to_vec(),
		EventBody2::Approve { record } => record
			.encode()
			.map_err(|_| bad(CorruptReason2::Payload))?
			.to_vec(),
		EventBody2::Conflict { observation } => observation
			.encode()
			.map_err(|_| bad(CorruptReason2::Payload))?
			.to_vec(),
		EventBody2::RetireTerminal { terminal } => {
			super::super::terminal::encode_terminal3(terminal)
				.map_err(|_| bad(CorruptReason2::Payload))?
				.to_vec()
		}
		EventBody2::Repair {
			tail_sha256,
			original_len,
			boundary,
			tail_len,
		} => {
			let mut p = Vec::with_capacity(52);
			p.extend(tail_sha256);
			p.extend(original_len.to_le_bytes());
			p.extend(boundary.to_le_bytes());
			p.extend(tail_len.to_le_bytes());
			p
		}
	};
	b[132..134].copy_from_slice(&(payload.len() as u16).to_le_bytes());
	b[144..144 + payload.len()].copy_from_slice(&payload);
	let sum = hash(b"", &b[..480]);
	b[480..].copy_from_slice(&sum);
	if decode_event2(&b)? != *e {
		return Err(bad(CorruptReason2::Payload));
	}
	Ok(b)
}
pub(in crate::storage::mutation_visibility::root2::provisioning) fn decode_event2(
	b: &[u8],
) -> Result<Event2, LogError2> {
	if b.len() != EVENT_BYTES2 {
		return Err(corrupt(0, CorruptReason2::Payload));
	}
	let seq = u64_at(b, 12);
	let bad = |r| corrupt(seq, r);
	if hash(b"", &b[..480]) != array::<32>(b, 480) {
		return Err(bad(CorruptReason2::Checksum));
	}
	if &b[..8] != MAGIC_V2 {
		return Err(bad(CorruptReason2::Magic));
	}
	if u16::from_le_bytes(array(b, 8)) != 1 {
		return Err(bad(CorruptReason2::Version));
	}
	let op = b[10];
	if !(RESERVE..=REPAIR).contains(&op) {
		return Err(bad(CorruptReason2::UnknownOp));
	}
	if b[11] != 0 || !zero(&b[134..144]) {
		return Err(bad(CorruptReason2::Reserved));
	}
	let op_id = array(b, 20);
	if op_id == [0; 16] {
		return Err(bad(CorruptReason2::ZeroOpId));
	}
	let identity = if op == REPAIR {
		if !zero(&b[36..132]) {
			return Err(bad(CorruptReason2::Header));
		}
		None
	} else {
		let i = RootIdentity2 {
			generation: array(b, 36),
			cluster: array(b, 52),
			pin: array(b, 68),
			admin_key: array(b, 100),
		};
		if i.generation == [0; 16]
			|| i.cluster == [0; 16]
			|| i.pin == [0; 32]
			|| i.admin_key == [0; 32]
		{
			return Err(bad(CorruptReason2::Header));
		}
		Some(i)
	};
	let n = u16::from_le_bytes(array(b, 132)) as usize;
	let valid = match op {
		RESERVE => n == 241,
		APPROVE | CONFLICT => n == 208,
		RETIRE => n == 156,
		REPAIR => n == 52,
		_ => false,
	};
	if !valid || !zero(&b[144 + n..480]) {
		return Err(bad(CorruptReason2::Payload));
	}
	let p = &b[144..144 + n];
	let body = match op {
		RESERVE => EventBody2::Reserve {
			manifest: AdminManifestThree3::decode(p).map_err(|_| bad(CorruptReason2::Payload))?,
		},
		APPROVE => EventBody2::Approve {
			record: BootstrapRecord2::decode(p).map_err(|_| bad(CorruptReason2::Payload))?,
		},
		CONFLICT => EventBody2::Conflict {
			observation: BootstrapObservation2::decode(p)
				.map_err(|_| bad(CorruptReason2::Payload))?,
		},
		RETIRE => EventBody2::RetireTerminal {
			terminal: super::super::terminal::decode_terminal3(p)
				.map_err(|_| bad(CorruptReason2::Payload))?,
		},
		_ => {
			let original_len = u64_at(p, 32);
			let boundary = u64_at(p, 40);
			let tail_len = u32::from_le_bytes(array(p, 48));
			if seq
				.checked_sub(1)
				.and_then(|x| x.checked_mul(EVENT_BYTES2 as u64))
				!= Some(boundary)
				|| boundary.checked_add(tail_len as u64) != Some(original_len)
				|| tail_len == 0
				|| tail_len as usize > MAX_TAIL2
			{
				return Err(bad(CorruptReason2::RepairBounds));
			}
			EventBody2::Repair {
				tail_sha256: array(p, 0),
				original_len,
				boundary,
				tail_len,
			}
		}
	};
	Ok(Event2 {
		seq,
		op_id,
		identity,
		body,
	})
}
pub(in crate::storage::mutation_visibility::root2::provisioning) fn fold_events2(
	log: &[u8],
) -> Result<LogState2, LogError2> {
	if log.len() > MAX_LOG_BYTES2 {
		return Err(LogError2::TooLong);
	}
	let whole = log.len() / EVENT_BYTES2;
	let mut state = LogState2 {
		roots: Vec::new(),
		last_seq: 0,
	};
	let mut ops = BTreeSet::new();
	let ambiguous = |boundary: usize| LogError2::Ambiguous {
		boundary: boundary as u64,
		tail_len: (log.len() - boundary) as u32,
		tail_sha256: hash(b"", &log[boundary..]),
	};
	for i in 0..whole {
		let at = i as u64 + 1;
		let event = match decode_event2(&log[i * EVENT_BYTES2..(i + 1) * EVENT_BYTES2]) {
			Ok(e) => e,
			Err(LogError2::Corrupt {
				reason: CorruptReason2::Checksum,
				..
			}) if i + 1 == whole => return Err(ambiguous(i * EVENT_BYTES2)),
			Err(LogError2::Corrupt { reason, .. }) => return Err(corrupt(at, reason)),
			Err(e) => return Err(e),
		};
		apply(&mut state, &mut ops, event, at)?;
	}
	if log.len() % EVENT_BYTES2 != 0 {
		return Err(ambiguous(whole * EVENT_BYTES2));
	}
	Ok(state)
}
