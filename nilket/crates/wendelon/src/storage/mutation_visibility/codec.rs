use super::{
	ApplyOutcome, AuthorityEvent, DecisionAuthority, DecisionLogId, DecisionRecord, MAX_DECISIONS,
	MAX_EPOCH_CUTS, MAX_MANIFEST_BYTES, MAX_STORED_DECISION_BYTES, ParentManifest,
	ReservedEpochCut, TerminalDecision, VisibilityError,
	authority::AuthorityState,
	encoding::{Reader, envelope, open_envelope},
};
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_AUTHORITY_EXTENSION_BYTES: usize = 67108864;
const SNAPSHOT_MAGIC: &[u8; 8] = b"VISAUTH1";
const EVENT_MAGIC: &[u8; 8] = b"VISEVENT";
impl AuthorityEvent {
	pub fn encode(&self) -> Result<Vec<u8>, VisibilityError> {
		let mut b = Vec::new();
		match self {
			Self::Decision {
				log,
				manifest,
				commitment,
				terminal,
			} => {
				let m = manifest.encode()?;
				b.push(1);
				b.extend_from_slice(&log.encode()?);
				b.extend_from_slice(&terminal.tags());
				b.extend_from_slice(&(m.len() as u32).to_le_bytes());
				b.extend_from_slice(&m);
				b.extend_from_slice(commitment);
			}
			Self::Coverage { log } => {
				b.push(2);
				b.extend_from_slice(&log.encode()?);
			}
		}
		envelope(EVENT_MAGIC, &b, MAX_MANIFEST_BYTES + 46)
	}
	pub fn decode(bytes: &[u8]) -> Result<Self, VisibilityError> {
		let payload = open_envelope(bytes, EVENT_MAGIC, MAX_MANIFEST_BYTES + 46)?;
		let mut r = Reader::new(payload);
		let tag = r.u8()?;
		let log = DecisionLogId::read(&mut r)?;
		let event = match tag {
			1 => {
				let terminal = TerminalDecision::read(&mut r)?;
				let n = r.u32()? as usize;
				if n > MAX_MANIFEST_BYTES {
					return Err(VisibilityError::LimitExceeded);
				}
				let manifest = ParentManifest::decode(r.take(n)?)?;
				let commitment = r.array()?;
				Self::Decision {
					log,
					manifest,
					commitment,
					terminal,
				}
			}
			2 => Self::Coverage { log },
			_ => return Err(VisibilityError::UnknownFormat),
		};
		r.finish()?;
		Ok(event)
	}
}
impl DecisionAuthority {
	/// Exact dormant extension, never a live daemon snapshot or generic root-WAL decoder.
	pub fn encode_extension(&self) -> Result<Vec<u8>, VisibilityError> {
		let s = self.state.lock().map_err(|_| VisibilityError::Poisoned)?;
		validate_state(&s)?;
		let size = 92 + s.cuts.len() * 36 + s.stored_bytes;
		let mut b = Vec::with_capacity(size);
		b.extend_from_slice(&s.generation);
		b.extend_from_slice(&s.applied.encode()?);
		b.extend_from_slice(&s.fingerprint);
		b.extend_from_slice(&s.initial_epoch.to_le_bytes());
		b.extend_from_slice(&[1, 1]);
		b.extend_from_slice(&(s.cuts.len() as u16).to_le_bytes());
		b.extend_from_slice(&(s.records.len() as u32).to_le_bytes());
		b.extend_from_slice(&(s.stored_bytes as u64).to_le_bytes());
		for c in &s.cuts {
			b.extend_from_slice(&c.prior_epoch.to_le_bytes());
			b.extend_from_slice(&c.new_epoch.to_le_bytes());
			b.extend_from_slice(&c.log.encode()?);
		}
		for record in s.records.values() {
			let encoded = record.encode()?;
			b.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
			b.extend_from_slice(&encoded);
		}
		envelope(SNAPSHOT_MAGIC, &b, MAX_AUTHORITY_EXTENSION_BYTES)
	}
	pub fn decode_extension(
		bytes: &[u8],
		expected_generation: [u8; 16],
	) -> Result<Self, VisibilityError> {
		let payload = open_envelope(bytes, SNAPSHOT_MAGIC, MAX_AUTHORITY_EXTENSION_BYTES)?;
		let mut r = Reader::new(payload);
		let generation = r.array()?;
		if generation != expected_generation {
			return Err(VisibilityError::WrongGeneration);
		}
		let applied = DecisionLogId::read(&mut r)?;
		let fingerprint = r.array()?;
		let initial_epoch = r.u64()?;
		if r.u8()? != 1 || r.u8()? != 1 {
			return Err(VisibilityError::UnknownFormat);
		}
		let cut_count = r.u16()? as usize;
		let count = r.u32()? as usize;
		let declared_bytes =
			usize::try_from(r.u64()?).map_err(|_| VisibilityError::LimitExceeded)?;
		if cut_count > MAX_EPOCH_CUTS
			|| count > MAX_DECISIONS
			|| declared_bytes > MAX_STORED_DECISION_BYTES
		{
			return Err(VisibilityError::LimitExceeded);
		}
		// Verify the complete declared byte budget before count-based allocations.
		let required = 92usize
			.checked_add(cut_count * 36)
			.and_then(|n| n.checked_add(declared_bytes))
			.ok_or(VisibilityError::LimitExceeded)?;
		if required != payload.len() || declared_bytes < count * 274 {
			return Err(VisibilityError::Corrupt);
		}
		let mut cuts = Vec::with_capacity(cut_count);
		for _ in 0..cut_count {
			cuts.push(ReservedEpochCut {
				prior_epoch: r.u64()?,
				new_epoch: r.u64()?,
				log: DecisionLogId::read(&mut r)?,
			});
		}
		let mut records = BTreeMap::new();
		let mut stored_bytes = 0usize;
		let mut previous = None;
		for _ in 0..count {
			let n = r.u32()? as usize;
			if !(270..=MAX_MANIFEST_BYTES + 58).contains(&n) {
				return Err(VisibilityError::LimitExceeded);
			}
			let record = DecisionRecord::decode(r.take(n)?)?;
			let key = record.manifest.identity.encode()?;
			if previous.is_some_and(|p| p >= key) {
				return Err(VisibilityError::Corrupt);
			}
			previous = Some(key);
			stored_bytes = stored_bytes
				.checked_add(n + 4)
				.ok_or(VisibilityError::LimitExceeded)?;
			records.insert(key, record);
		}
		r.finish()?;
		if stored_bytes != declared_bytes {
			return Err(VisibilityError::Corrupt);
		}
		let state = AuthorityState {
			generation,
			applied,
			fingerprint,
			initial_epoch,
			cuts,
			records,
			stored_bytes,
		};
		validate_state(&state)?;
		Ok(Self::from_state(state))
	}
	/// Only the dormant event decoder is used; corrupt input never reaches the state lock.
	pub fn replay_event(&self, bytes: &[u8]) -> Result<ApplyOutcome, VisibilityError> {
		self.apply(&AuthorityEvent::decode(bytes)?)
	}
}
fn validate_state(s: &AuthorityState) -> Result<(), VisibilityError> {
	if s.generation == [0; 16] || s.initial_epoch == 0 {
		return Err(VisibilityError::InvalidIdentity);
	}
	s.applied.encode()?;
	if s.records.len() > MAX_DECISIONS
		|| s.cuts.len() > MAX_EPOCH_CUTS
		|| s.stored_bytes > MAX_STORED_DECISION_BYTES
	{
		return Err(VisibilityError::LimitExceeded);
	}
	let mut epoch = s.initial_epoch;
	let mut index = 0;
	for c in &s.cuts {
		c.log.encode()?;
		if c.prior_epoch != epoch
			|| c.new_epoch <= epoch
			|| c.log.index <= index
			|| c.log.index > s.applied.index
			|| (c.log.index == s.applied.index && c.log != s.applied)
		{
			return Err(VisibilityError::InvalidCut);
		}
		epoch = c.new_epoch;
		index = c.log.index;
	}
	let mut sum = 0usize;
	let mut indices = BTreeSet::new();
	for (key, record) in &s.records {
		if record.manifest.identity.generation != s.generation
			|| record.manifest.identity.encode()? != *key
		{
			return Err(VisibilityError::WrongGeneration);
		}
		record.first_log.encode()?;
		if record.first_log.index > s.applied.index
			|| (record.first_log.index == s.applied.index && record.first_log != s.applied)
			|| !indices.insert(record.first_log.index)
		{
			return Err(VisibilityError::InvalidCoordinate);
		}
		sum = sum
			.checked_add(record.stored_bytes()?)
			.ok_or(VisibilityError::LimitExceeded)?;
	}
	if sum != s.stored_bytes {
		return Err(VisibilityError::Corrupt);
	}
	Ok(())
}
