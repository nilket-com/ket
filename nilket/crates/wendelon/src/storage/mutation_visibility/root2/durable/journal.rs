use super::*;
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone, PartialEq, Eq)]
pub struct JournalIdentity2 {
	pub generation: [u8; 16],
	pub cluster: [u8; 16],
	pub identity: MutationIdentity,
	pub claim: Digest2,
	pub reservation: Log2,
}
#[derive(Clone, PartialEq, Eq)]
pub enum UnverifiedJournalStep2 {
	Prepared(ClaimProjection2),
	LocalChildDurable(UnverifiedReceipt2),
	WitnessReady(UnverifiedWitness2),
	TerminalObserved { result: Vec<u8>, processed: Log2 },
	OutcomeDurable(UnverifiedOutcome2),
	ProjectionComplete { decision: Log2, digest: Digest2 },
	WitnessRetired,
}
#[derive(Clone, PartialEq, Eq)]
pub struct UnverifiedJournal2 {
	pub owner: JournalIdentity2,
	pub sequence: u64,
	pub previous: Digest2,
	pub step: UnverifiedJournalStep2,
}
opaque!(JournalIdentity2, UnverifiedJournalStep2, UnverifiedJournal2);
impl JournalIdentity2 {
	fn bytes(&self) -> Result2<Vec<u8>> {
		nonzero16(self.generation)?;
		nonzero16(self.cluster)?;
		check(self.generation == self.identity.generation)?;
		let mut b = Vec::new();
		b.extend(self.generation);
		b.extend(self.cluster);
		put_id(&mut b, self.identity)?;
		b.extend(self.claim);
		log(&mut b, self.reservation)?;
		Ok(b)
	}
	pub fn locator(&self) -> Result2<String> {
		let mut b = b"VIS-JOURNAL-LOCATOR-2\0".to_vec();
		b.extend(self.bytes()?);
		Ok(sha(&b).iter().map(|b| format!("{b:02x}")).collect())
	}
	fn matches_claim(&self, c: &ClaimProjection2) -> Result2<()> {
		check(c.manifest.identity == self.identity && c.digest()? == self.claim)
	}
	fn matches_receipt(&self, r: &UnverifiedReceipt2) -> Result2<()> {
		check(
			(r.generation, r.cluster, r.identity, r.claim, r.reservation)
				== (
					self.generation,
					self.cluster,
					self.identity,
					self.claim,
					self.reservation,
				),
		)
	}
	fn matches_outcome(&self, o: &UnverifiedOutcome2) -> Result2<()> {
		check(
			(o.generation, o.cluster, o.identity, o.claim)
				== (self.generation, self.cluster, self.identity, self.claim),
		)?;
		check(o.terminal()?.1.reservation == self.reservation)
	}
}
impl UnverifiedJournalStep2 {
	fn tag(&self) -> u8 {
		match self {
			Self::Prepared(_) => 1,
			Self::LocalChildDurable(_) => 2,
			Self::WitnessReady(_) => 3,
			Self::TerminalObserved { .. } => 4,
			Self::OutcomeDurable(_) => 5,
			Self::ProjectionComplete { .. } => 6,
			Self::WitnessRetired => 7,
		}
	}
	fn body(&self) -> Result2<Vec<u8>> {
		let mut b = Vec::new();
		match self {
			Self::Prepared(c) => blob(&mut b, &c.encode()?)?,
			Self::LocalChildDurable(r) => blob(&mut b, &r.encode()?)?,
			Self::WitnessReady(w) => blob(&mut b, &w.encode()?)?,
			Self::TerminalObserved { result, processed } => {
				blob(&mut b, result)?;
				log(&mut b, *processed)?;
			}
			Self::OutcomeDurable(o) => blob(&mut b, &o.encode()?)?,
			Self::ProjectionComplete { decision, digest } => {
				log(&mut b, *decision)?;
				b.extend(digest);
			}
			Self::WitnessRetired => {}
		}
		Ok(b)
	}
}
impl UnverifiedJournal2 {
	fn validate(&self) -> Result2<()> {
		self.owner.bytes()?;
		check(self.sequence > 0 && ((self.sequence == 1) == (self.previous == [0; 32])))?;
		match &self.step {
			UnverifiedJournalStep2::Prepared(c) => self.owner.matches_claim(c)?,
			UnverifiedJournalStep2::LocalChildDurable(r) => self.owner.matches_receipt(r)?,
			UnverifiedJournalStep2::WitnessReady(w) => {
				self.owner.matches_claim(&w.claim)?;
				check(
					w.reservation == self.owner.reservation
						&& w.receipts.iter().all(|r| r.cluster == self.owner.cluster),
				)?;
			}
			UnverifiedJournalStep2::TerminalObserved { result, processed } => {
				let (g, c, l, v) = ApplyResult2::decode(result)?;
				check((g, c, l) == (self.owner.generation, self.owner.cluster, *processed))?;
				let ApplyResult2::Parent { record, .. } = v else {
					return Err(RootError2::Codec);
				};
				self.owner.matches_claim(&record.claim)?;
				check(
					record.reservation == self.owner.reservation
						&& record
							.terminal
							.is_some_and(|t| t.first().index <= processed.index),
				)?;
			}
			UnverifiedJournalStep2::OutcomeDurable(o) => self.owner.matches_outcome(o)?,
			UnverifiedJournalStep2::ProjectionComplete { decision, .. } => {
				super::super::codec::valid_log(*decision)?
			}
			UnverifiedJournalStep2::WitnessRetired => {}
		}
		Ok(())
	}
	pub fn encode(&self) -> Result2<Vec<u8>> {
		self.validate()?;
		let mut b = Vec::new();
		b.extend(self.owner.generation);
		b.extend(self.owner.cluster);
		b.extend(self.sequence.to_le_bytes());
		b.extend(self.previous);
		put_id(&mut b, self.owner.identity)?;
		b.extend(self.owner.claim);
		log(&mut b, self.owner.reservation)?;
		b.push(self.step.tag());
		blob(&mut b, &self.step.body()?)?;
		wrap(b"VISJRN02", &b, JOURNAL_MAX)
	}
	pub fn decode(bytes: &[u8]) -> Result2<Self> {
		let mut r = open(bytes, b"VISJRN02", JOURNAL_MAX)?;
		let generation = r.array()?;
		let cluster = r.array()?;
		let sequence = r.u64()?;
		let previous = r.array()?;
		let id = identity(&mut r)?;
		let claim = r.array()?;
		let reservation = r.log()?;
		let tag = r.u8()?;
		let mut body = Read2::new(r.blob(JOURNAL_MAX - 250)?);
		r.end()?;
		let step = match tag {
			1 => UnverifiedJournalStep2::Prepared(ClaimProjection2::decode(
				body.blob(CLAIM_BYTES2)?,
			)?),
			2 => UnverifiedJournalStep2::LocalChildDurable(UnverifiedReceipt2::decode(
				body.blob(4096)?,
			)?),
			3 => UnverifiedJournalStep2::WitnessReady(UnverifiedWitness2::decode(
				body.blob(WITNESS_MAX)?,
			)?),
			4 => UnverifiedJournalStep2::TerminalObserved {
				result: body.blob(COMMAND_BYTES2 + 48)?.to_vec(),
				processed: body.log()?,
			},
			5 => UnverifiedJournalStep2::OutcomeDurable(UnverifiedOutcome2::decode(
				body.blob(OUTCOME_MAX)?,
			)?),
			6 => UnverifiedJournalStep2::ProjectionComplete {
				decision: body.log()?,
				digest: body.array()?,
			},
			7 => UnverifiedJournalStep2::WitnessRetired,
			_ => return Err(RootError2::Codec),
		};
		body.end()?;
		let v = Self {
			owner: JournalIdentity2 {
				generation,
				cluster,
				identity: id,
				claim,
				reservation,
			},
			sequence,
			previous,
			step,
		};
		v.validate()?;
		Ok(v)
	}
}
opaque!(JournalChainReport2);
/// Caller-declared bounded synthetic enumeration contract, not a live storage quota.
#[derive(Debug, Clone, Copy)]
pub struct JournalLoadBudget2 {
	pub chains: usize,
	pub open_handles: usize,
	pub records_per_chain: usize,
	pub total_bytes_per_chain: usize,
}
pub struct JournalChainReport2 {
	pub locator: String,
	pub validated_records: usize,
	pub unavailable: bool,
	pub damaged_suffix: Vec<u8>,
}
/// Private bounded parser: no IO, no filename enumeration and no acceptance/readiness token.
/// An absent/failed enumeration is an error, never an empty set of pending work.
pub fn inspect_journal_objects2(
	objects: Option<&[(String, Vec<u8>)]>,
	budget: JournalLoadBudget2,
) -> Result2<Vec<JournalChainReport2>> {
	let objects = objects.ok_or(RootError2::Codec)?;
	if budget.chains == 0
		|| budget.chains > PARENT_SLOTS2
		|| budget.open_handles == 0
		|| budget.records_per_chain == 0
		|| budget.total_bytes_per_chain == 0
		|| objects.len() > budget.chains
		|| objects.len() > budget.open_handles
	{
		return Err(RootError2::Limit);
	}
	let mut seen = BTreeSet::new();
	let mut owners = BTreeSet::new();
	let mut out = Vec::new();
	for (locator, bytes) in objects {
		check(
			locator.len() == 64
				&& locator
					.bytes()
					.all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
				&& seen.insert(locator.clone()),
		)?;
		if bytes.len() > budget.total_bytes_per_chain {
			return Err(RootError2::Limit);
		}
		// A duplicate full tuple is a collection conflict even if its second locator is wrong.
		if let Some(prefix) = bytes.get(..16) {
			let n = u32::from_le_bytes(prefix[12..16].try_into().map_err(|_| RootError2::Codec)?)
				as usize;
			if n <= JOURNAL_MAX - 48 {
				if let Some(raw) = bytes.get(..n + 48) {
					if let Ok(record) = UnverifiedJournal2::decode(raw) {
						check(owners.insert(record.owner.bytes()?))?;
					}
				}
			}
		}

		let mut pos = 0;
		let mut count = 0;
		let mut previous = [0; 32];
		let mut owner = None;
		let mut last_tag = 0;
		let mut bodies = BTreeMap::new();
		let mut terminal = None;
		while pos < bytes.len() {
			if count >= budget.records_per_chain {
				return Err(RootError2::Limit);
			}
			let parsed = (|| {
				let prefix = bytes
					.get(pos..pos.checked_add(16).ok_or(RootError2::Overflow)?)
					.ok_or(RootError2::Codec)?;
				let n =
					u32::from_le_bytes(prefix[12..16].try_into().map_err(|_| RootError2::Codec)?)
						as usize;
				if n > JOURNAL_MAX - 48 {
					return Err(RootError2::Limit);
				}
				let end = pos
					.checked_add(n)
					.and_then(|x| x.checked_add(48))
					.ok_or(RootError2::Overflow)?;
				let raw = bytes.get(pos..end).ok_or(RootError2::Codec)?;
				let record = UnverifiedJournal2::decode(raw)?;
				check(record.sequence == count as u64 + 1 && record.previous == previous)?;
				check(owner.as_ref().is_none_or(|o| o == &record.owner))?;
				check(record.owner.locator()? == *locator)?;
				if count == 0 {
					check(matches!(record.step, UnverifiedJournalStep2::Prepared(_)))?;
				}
				let tag = record.step.tag();
				check(tag >= last_tag)?;
				let discriminator = match &record.step {
					UnverifiedJournalStep2::LocalChildDurable(r) => (r.child, r.node),
					_ => (
						u32::MAX,
						NodeRef2 {
							node: 0,
							incarnation: [0; 16],
						},
					),
				};
				let key = (tag, discriminator);
				let body = record.step.body()?;
				check(bodies.get(&key).is_none_or(|old| old == &body))?;
				match &record.step {
					UnverifiedJournalStep2::TerminalObserved { result, .. } => {
						let (_, _, _, ApplyResult2::Parent { record, .. }) =
							ApplyResult2::decode(result)?
						else {
							return Err(RootError2::Codec);
						};
						terminal = record.terminal;
					}
					UnverifiedJournalStep2::OutcomeDurable(o) => {
						check(terminal == o.terminal()?.1.terminal)?
					}
					UnverifiedJournalStep2::ProjectionComplete { decision, .. } => {
						check(terminal.is_some_and(|t| t.first() == *decision))?
					}
					UnverifiedJournalStep2::WitnessRetired => check(last_tag >= 6)?,
					_ => {}
				}
				bodies.insert(key, body);
				owner = Some(record.owner);
				previous = sha(raw);
				last_tag = tag;
				Ok(end)
			})();
			match parsed {
				Ok(end) => {
					pos = end;
					count += 1;
				}
				Err(RootError2::Limit | RootError2::Overflow) => return Err(RootError2::Limit),
				Err(_) => break,
			}
		}
		out.push(JournalChainReport2 {
			locator: locator.clone(),
			validated_records: count,
			unavailable: pos < bytes.len() || count == 0,
			damaged_suffix: bytes[pos..].to_vec(),
		});
	}
	Ok(out)
}
