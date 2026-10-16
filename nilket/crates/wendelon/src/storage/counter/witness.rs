//! Versioned witnesses. Legacy postcard bytes remain owned by the legacy codec.
use super::*;

#[derive(Clone)]
pub(super) struct StoredWitness {
	pub(super) body: CounterFinalizationWitness,
	pub(super) origin: Option<NodeId>,
}
impl std::ops::Deref for StoredWitness {
	type Target = CounterFinalizationWitness;
	fn deref(&self) -> &Self::Target {
		&self.body
	}
}
impl std::ops::DerefMut for StoredWitness {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.body
	}
}
fn invalid() -> std::io::Error {
	CounterFormatError::InvalidOriginFrame.io()
}
fn put32(bytes: &mut [u8], at: usize, value: u32) {
	bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
}
fn put64(bytes: &mut [u8], at: usize, value: u64) {
	bytes[at..at + 8].copy_from_slice(&value.to_le_bytes());
}
fn get32(bytes: &[u8], at: usize) -> u32 {
	u32::from_le_bytes(bytes[at..at + 4].try_into().expect("bounded witness field"))
}
fn get64(bytes: &[u8], at: usize) -> u64 {
	u64::from_le_bytes(bytes[at..at + 8].try_into().expect("bounded witness field"))
}

pub(super) fn encode(w: &StoredWitness) -> std::io::Result<Vec<u8>> {
	let origin = w.origin.ok_or_else(invalid)?;
	if origin.id == 0 || origin._reserved != 0 || w.op_id == 0 {
		return Err(invalid());
	}
	let keylen = u32::try_from(w.key.0.len()).map_err(|_| invalid())?;
	let ac = u32::try_from(w.accepted.len()).map_err(|_| invalid())?;
	let cc = u32::try_from(w.committed.len()).map_err(|_| invalid())?;
	let mut seen = HashSet::new();
	for id in &w.accepted {
		if id.id == 0 || id._reserved != 0 || !seen.insert(id.id) {
			return Err(invalid());
		}
	}
	let mut committed: Vec<u32> = w.committed.iter().map(|id| id.id).collect();
	for id in &w.committed {
		if id._reserved != 0 || !seen.contains(&id.id) {
			return Err(invalid());
		}
	}
	committed.sort_unstable();
	let len = 104usize
		.checked_add(keylen as usize)
		.and_then(|len| {
			(ac as usize)
				.checked_add(cc as usize)
				.and_then(|n| n.checked_mul(4))
				.and_then(|n| len.checked_add(n))
		})
		.ok_or_else(invalid)?;
	let total = u32::try_from(len).map_err(|_| invalid())?;
	let mut bytes = vec![0; len];
	bytes[..8].copy_from_slice(b"CTRWIT02");
	bytes[8..10].copy_from_slice(&2u16.to_le_bytes());
	put32(&mut bytes, 12, total);
	put32(&mut bytes, 16, origin.id);
	put64(&mut bytes, 24, w.op_id);
	bytes[32..56].copy_from_slice(&w.write_key);
	put32(&mut bytes, 56, keylen);
	put64(&mut bytes, 64, w.value);
	put64(&mut bytes, 72, w.previous);
	put64(&mut bytes, 80, w.commit_hlc.physical_ms);
	put32(&mut bytes, 88, w.commit_hlc.logical);
	put32(&mut bytes, 92, w.commit_hlc.node_id);
	put32(&mut bytes, 96, ac);
	put32(&mut bytes, 100, cc);
	bytes[104..104 + keylen as usize].copy_from_slice(&w.key.0);
	let mut at = 104 + keylen as usize;
	for id in w.accepted.iter().map(|id| id.id).chain(committed) {
		put32(&mut bytes, at, id);
		at += 4;
	}
	Ok(bytes)
}

pub(super) fn decode(bytes: &[u8]) -> std::io::Result<StoredWitness> {
	if bytes.len() < 104
		|| &bytes[..8] != b"CTRWIT02"
		|| bytes[8..12] != [2, 0, 0, 0]
		|| bytes[20..24] != [0; 4]
		|| bytes[60..64] != [0; 4]
		|| usize::try_from(get32(bytes, 12)).map_err(|_| invalid())? != bytes.len()
	{
		return Err(invalid());
	}
	let origin = get32(bytes, 16);
	let op_id = get64(bytes, 24);
	if origin == 0 || op_id == 0 {
		return Err(invalid());
	}
	let keylen = get32(bytes, 56) as usize;
	let ac = get32(bytes, 96) as usize;
	let cc = get32(bytes, 100) as usize;
	let expected = 104usize
		.checked_add(keylen)
		.and_then(|n| {
			ac.checked_add(cc)
				.and_then(|c| c.checked_mul(4))
				.and_then(|c| n.checked_add(c))
		})
		.ok_or_else(invalid)?;
	if expected != bytes.len() {
		return Err(invalid());
	}
	let key = CounterKey(bytes[104..104 + keylen].to_vec());
	let mut at = 104 + keylen;
	let mut accepted = Vec::with_capacity(ac);
	let mut seen = HashSet::new();
	for _ in 0..ac {
		let id = get32(bytes, at);
		at += 4;
		if id == 0 || !seen.insert(id) {
			return Err(invalid());
		}
		accepted.push(NodeId { id, _reserved: 0 });
	}
	let mut committed = HashSet::new();
	let mut previous_id = 0;
	for _ in 0..cc {
		let id = get32(bytes, at);
		at += 4;
		if id <= previous_id || !seen.contains(&id) {
			return Err(invalid());
		}
		previous_id = id;
		committed.insert(NodeId { id, _reserved: 0 });
	}
	Ok(StoredWitness {
		origin: Some(NodeId {
			id: origin,
			_reserved: 0,
		}),
		body: CounterFinalizationWitness {
			op_id,
			write_key: bytes[32..56].try_into().expect("fixed key"),
			key,
			value: get64(bytes, 64),
			previous: get64(bytes, 72),
			commit_hlc: HybridTimestamp {
				physical_ms: get64(bytes, 80),
				logical: get32(bytes, 88),
				node_id: get32(bytes, 92),
			},
			accepted,
			committed,
		},
	})
}
