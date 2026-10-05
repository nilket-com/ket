use super::{ParentManifest, VisibilityError, encoding::Reader, redacted_debug};
use std::{collections::BTreeMap, sync::Mutex};

pub const MAX_DECISIONS: usize = 32768;
pub const MAX_STORED_DECISION_BYTES: usize = 66846720;
pub const MAX_EPOCH_CUTS: usize = 4096;
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct DecisionLogId {
	pub term: u64,
	pub node: u32,
	pub index: u64,
}
redacted_debug!(DecisionLogId, "DecisionLogId");
impl DecisionLogId {
	pub fn encode(&self) -> Result<[u8; 20], VisibilityError> {
		if self.term == 0 || self.node == 0 || self.index == 0 {
			return Err(VisibilityError::InvalidCoordinate);
		}
		let mut b = [0; 20];
		b[..8].copy_from_slice(&self.term.to_le_bytes());
		b[8..12].copy_from_slice(&self.node.to_le_bytes());
		b[12..].copy_from_slice(&self.index.to_le_bytes());
		Ok(b)
	}
	pub(super) fn read(r: &mut Reader<'_>) -> Result<Self, VisibilityError> {
		let v = Self {
			term: r.u64()?,
			node: r.u32()?,
			index: r.u64()?,
		};
		v.encode()?;
		Ok(v)
	}
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RefusalCause {
	EpochCut = 1,
	InvalidAdmission = 2,
	ExplicitPolicy = 3,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalDecision {
	Accepted,
	Refused(RefusalCause),
}
impl TerminalDecision {
	pub(super) fn tags(self) -> [u8; 2] {
		match self {
			Self::Accepted => [1, 0],
			Self::Refused(c) => [2, c as u8],
		}
	}
	pub(super) fn read(r: &mut Reader<'_>) -> Result<Self, VisibilityError> {
		match (r.u8()?, r.u8()?) {
			(1, 0) => Ok(Self::Accepted),
			(2, 1) => Ok(Self::Refused(RefusalCause::EpochCut)),
			(2, 2) => Ok(Self::Refused(RefusalCause::InvalidAdmission)),
			(2, 3) => Ok(Self::Refused(RefusalCause::ExplicitPolicy)),
			_ => Err(VisibilityError::InvalidDecision),
		}
	}
}
#[derive(Clone, PartialEq, Eq)]
pub struct DecisionRecord {
	pub manifest: ParentManifest,
	pub commitment: [u8; 32],
	pub first_log: DecisionLogId,
	pub terminal: TerminalDecision,
}
redacted_debug!(DecisionRecord, "DecisionRecord");
impl DecisionRecord {
	pub(super) fn encode(&self) -> Result<Vec<u8>, VisibilityError> {
		let m = self.manifest.encode()?;
		let mut b = Vec::with_capacity(m.len() + 58);
		b.extend_from_slice(&(m.len() as u32).to_le_bytes());
		b.extend_from_slice(&m);
		b.extend_from_slice(&self.commitment);
		b.extend_from_slice(&self.first_log.encode()?);
		b.extend_from_slice(&self.terminal.tags());
		Ok(b)
	}
	pub(super) fn decode(bytes: &[u8]) -> Result<Self, VisibilityError> {
		if bytes.len() > super::MAX_MANIFEST_BYTES + 58 {
			return Err(VisibilityError::LimitExceeded);
		}
		let mut r = Reader::new(bytes);
		let len = r.u32()? as usize;
		if len > super::MAX_MANIFEST_BYTES {
			return Err(VisibilityError::LimitExceeded);
		}
		let manifest = ParentManifest::decode(r.take(len)?)?;
		let commitment = r.array()?;
		let first_log = DecisionLogId::read(&mut r)?;
		let terminal = TerminalDecision::read(&mut r)?;
		r.finish()?;
		Ok(Self {
			manifest,
			commitment,
			first_log,
			terminal,
		})
	}
	pub fn stored_bytes(&self) -> Result<usize, VisibilityError> {
		Ok(self.manifest.validate()? + 62)
	}
}
#[derive(Clone, PartialEq, Eq)]
pub enum AuthorityEvent {
	Decision {
		log: DecisionLogId,
		manifest: ParentManifest,
		commitment: [u8; 32],
		terminal: TerminalDecision,
	},
	Coverage {
		log: DecisionLogId,
	},
}
redacted_debug!(AuthorityEvent, "AuthorityEvent");
impl AuthorityEvent {
	pub fn log(&self) -> DecisionLogId {
		match self {
			Self::Decision { log, .. } | Self::Coverage { log } => *log,
		}
	}
}
#[derive(Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
	Recorded(DecisionRecord),
	Replayed(DecisionRecord),
	Conflict,
	CapacityUnavailable,
	Covered,
	AlreadyProcessed,
}
redacted_debug!(ApplyOutcome, "ApplyOutcome");
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ReservedEpochCut {
	pub prior_epoch: u64,
	pub new_epoch: u64,
	pub log: DecisionLogId,
}
redacted_debug!(ReservedEpochCut, "ReservedEpochCut");
#[derive(Clone, PartialEq, Eq)]
pub(super) struct AuthorityState {
	pub generation: [u8; 16],
	pub applied: DecisionLogId,
	pub fingerprint: [u8; 32],
	pub initial_epoch: u64,
	pub cuts: Vec<ReservedEpochCut>,
	pub records: BTreeMap<[u8; 73], DecisionRecord>,
	pub stored_bytes: usize,
}
/// A single lock linearizes decisions/coverage. No provider, I/O or await inside it.
/// No live WAL/Finalize path is attached. Unknown means validated absence, never decode failure.
pub struct DecisionAuthority {
	pub(super) state: Mutex<AuthorityState>,
	count_limit: usize,
	byte_limit: usize,
}
redacted_debug!(DecisionAuthority, "DecisionAuthority");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityUsage {
	pub decisions: usize,
	pub stored_bytes: usize,
}
impl DecisionAuthority {
	/// Starts at an explicitly supplied trusted applied coordinate, not an implicit empty recovery.
	pub fn new(
		generation: [u8; 16],
		initial_epoch: u64,
		applied: DecisionLogId,
	) -> Result<Self, VisibilityError> {
		if generation == [0; 16] || initial_epoch == 0 {
			return Err(VisibilityError::InvalidIdentity);
		}
		let event = AuthorityEvent::Coverage { log: applied };
		let fingerprint = *blake3::hash(&event.encode()?).as_bytes();
		Ok(Self::from_state(AuthorityState {
			generation,
			applied,
			fingerprint,
			initial_epoch,
			cuts: Vec::new(),
			records: BTreeMap::new(),
			stored_bytes: 0,
		}))
	}
	pub(super) fn from_state(state: AuthorityState) -> Self {
		Self {
			state: Mutex::new(state),
			count_limit: MAX_DECISIONS,
			byte_limit: MAX_STORED_DECISION_BYTES,
		}
	}
	#[cfg(test)]
	pub(super) fn with_limits(mut self, count: usize, bytes: usize) -> Self {
		assert!(count <= MAX_DECISIONS && bytes <= MAX_STORED_DECISION_BYTES);
		self.count_limit = count;
		self.byte_limit = bytes;
		self
	}
	pub fn usage(&self) -> Result<AuthorityUsage, VisibilityError> {
		let s = self.state.lock().map_err(|_| VisibilityError::Poisoned)?;
		Ok(AuthorityUsage {
			decisions: s.records.len(),
			stored_bytes: s.stored_bytes,
		})
	}
	pub fn lookup(
		&self,
		id: &super::MutationIdentity,
	) -> Result<Option<DecisionRecord>, VisibilityError> {
		let key = id.encode()?;
		let s = self.state.lock().map_err(|_| VisibilityError::Poisoned)?;
		if id.generation != s.generation {
			return Err(VisibilityError::WrongGeneration);
		}
		Ok(s.records.get(&key).cloned())
	}
	pub fn applied(&self) -> Result<DecisionLogId, VisibilityError> {
		Ok(self
			.state
			.lock()
			.map_err(|_| VisibilityError::Poisoned)?
			.applied)
	}
	pub fn apply(&self, event: &AuthorityEvent) -> Result<ApplyOutcome, VisibilityError> {
		// Bounded validation/serialization precedes the lock; no secret provider needed.
		let encoded = event.encode()?;
		let fingerprint = *blake3::hash(&encoded).as_bytes();
		let log = event.log();
		let key = match event {
			AuthorityEvent::Decision { manifest, .. } => Some(manifest.identity.encode()?),
			_ => None,
		};
		let mut s = self.state.lock().map_err(|_| VisibilityError::Poisoned)?;
		if let AuthorityEvent::Decision { manifest, .. } = event {
			if manifest.identity.generation != s.generation {
				return Err(VisibilityError::WrongGeneration);
			}
		}
		if log.index < s.applied.index {
			return Err(VisibilityError::UnorderedReplay);
		}
		if log.index == s.applied.index {
			return if log == s.applied && fingerprint == s.fingerprint {
				Ok(ApplyOutcome::AlreadyProcessed)
			} else {
				Err(VisibilityError::CoordinateConflict)
			};
		}
		let outcome = match event {
			AuthorityEvent::Coverage { .. } => ApplyOutcome::Covered,
			AuthorityEvent::Decision {
				manifest,
				commitment,
				terminal,
				..
			} => {
				let key = key.expect("decision key validated");
				if let Some(old) = s.records.get(&key) {
					if old.manifest == *manifest && old.commitment == *commitment {
						ApplyOutcome::Replayed(old.clone())
					} else {
						ApplyOutcome::Conflict
					}
				} else {
					let record = DecisionRecord {
						manifest: manifest.clone(),
						commitment: *commitment,
						first_log: log,
						terminal: *terminal,
					};
					let bytes = record.stored_bytes()?;
					match s.stored_bytes.checked_add(bytes) {
						Some(next)
							if s.records.len() < self.count_limit && next <= self.byte_limit =>
						{
							s.records.insert(key, record.clone());
							s.stored_bytes = next;
							ApplyOutcome::Recorded(record)
						}
						_ => ApplyOutcome::CapacityUnavailable,
					}
				}
			}
		};
		s.applied = log;
		s.fingerprint = fingerprint;
		Ok(outcome)
	}
	/// Synthetic retained history for the dormant codec, not an epoch policy implementation.
	pub fn add_reserved_cut(&self, cut: ReservedEpochCut) -> Result<(), VisibilityError> {
		cut.log.encode()?;
		let mut s = self.state.lock().map_err(|_| VisibilityError::Poisoned)?;
		let previous = s.cuts.last();
		let prior = previous.map_or(s.initial_epoch, |c| c.new_epoch);
		if cut.prior_epoch != prior
			|| cut.new_epoch <= prior
			|| cut.log.index > s.applied.index
			|| (cut.log.index == s.applied.index && cut.log != s.applied)
			|| previous.is_some_and(|c| c.log.index >= cut.log.index)
		{
			return Err(VisibilityError::InvalidCut);
		}
		if s.cuts.len() >= MAX_EPOCH_CUTS {
			return Err(VisibilityError::LimitExceeded);
		}
		s.cuts.push(cut);
		Ok(())
	}
}
