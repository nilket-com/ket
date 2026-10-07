use super::*;
pub(super) type ProgressKey = (u8, u32, NodeRef2);
#[derive(Clone)]
pub(super) struct PriorBody {
	pub(super) offset: u64,
	pub(super) len: usize,
	pub(super) digest: Digest2,
}
#[derive(Clone, Default)]
pub(super) struct ChainState {
	pub(super) endpoint: u64,
	pub(super) count: u64,
	pub(super) previous: Digest2,
	pub(super) last_tag: u8,
	pub(super) bodies: BTreeMap<ProgressKey, PriorBody>,
	pub(super) terminal: Option<Terminal2>,
	pub(super) terminal_result: Option<Digest2>,
	pub(super) claim: Option<ClaimProjection2>,
	pub(super) damaged: Option<(u64, u64)>,
}
pub(super) fn progress_key(step: &UnverifiedJournalStep2) -> ProgressKey {
	match step {
		UnverifiedJournalStep2::LocalChildDurable(r) => (2, r.child, r.node),
		_ => (
			step.tag(),
			u32::MAX,
			NodeRef2 {
				node: 0,
				incarnation: [0; 16],
			},
		),
	}
}
impl ChainState {
	pub(super) fn validate(
		&self,
		entry: &ParentEntry,
		record: &UnverifiedJournal2,
	) -> StoreResult<()> {
		require(
			record.owner == entry.owner
				&& record.sequence == self.count + 1
				&& record.previous == self.previous,
		)?;
		require(record.step.tag() >= self.last_tag && self.count < 4096)?;
		if self.count == 0 {
			require(matches!(record.step, UnverifiedJournalStep2::Prepared(_)))?
		}
		match &record.step {
			UnverifiedJournalStep2::Prepared(c) => {
				let raw = c.encode()?;
				require(
					raw.len() == entry.credit.claim as usize && sha(&raw) == entry.credit.plan,
				)?;
				let n = if entry.credit.role == 1 {
					c.plans.iter().map(|p| p.nodes.len()).sum::<usize>()
				} else {
					c.plans
						.iter()
						.filter(|p| p.nodes.contains(&entry.credit.local))
						.count()
				};
				require(
					n == entry.credit.slots as usize
						&& c.manifest.children.len() == entry.credit.children as usize
						&& c.manifest
							.children
							.iter()
							.map(|c| c.versions.len())
							.sum::<usize>() == entry.credit.versions as usize,
				)?;
				if entry.credit.role == 1 {
					require(
						entry.credit.local.node == c.original.lease.holder
							&& entry.credit.local.incarnation == c.original.incarnation,
					)?
				}
			}
			UnverifiedJournalStep2::LocalChildDurable(r) => {
				let c = self.claim.as_ref().ok_or(StoreError::Unavailable)?;
				let plan = c
					.plans
					.iter()
					.find(|p| p.child == r.child)
					.ok_or(StoreError::Unavailable)?;
				require(
					plan.nodes.contains(&r.node)
						&& (entry.credit.role == 1 || r.node == entry.credit.local),
				)?;
			}
			UnverifiedJournalStep2::WitnessReady(w) => {
				require(entry.credit.role == 1 && Some(&w.claim) == self.claim.as_ref())?;
				require(w.receipts.len() <= entry.credit.slots as usize)?;
				let c = self.claim.as_ref().ok_or(StoreError::Unavailable)?;
				for r in &w.receipts {
					require(
						c.plans
							.iter()
							.any(|p| p.child == r.child && p.nodes.contains(&r.node)),
					)?
				}
			}
			UnverifiedJournalStep2::TerminalObserved { result, .. } => {
				require(entry.credit.role == 1)?;
				let (_, _, _, ApplyResult2::Parent { record, .. }) = ApplyResult2::decode(result)?
				else {
					return Err(StoreError::Unavailable);
				};
				require(record.terminal.is_some())?;
			}
			UnverifiedJournalStep2::OutcomeDurable(o) => {
				require(
					entry.credit.role == 1
						&& self.terminal == o.terminal()?.1.terminal
						&& self.terminal.is_some()
						&& self.terminal_result == Some(sha(&o.result)),
				)?;
				require(o.response.len() <= entry.credit.response as usize)?;
			}
			UnverifiedJournalStep2::ProjectionComplete { decision, .. } => require(
				entry.credit.role == 1
					&& self.last_tag >= 5
					&& self.terminal.is_some_and(|t| t.first() == *decision),
			)?,
			UnverifiedJournalStep2::WitnessRetired => {
				require(entry.credit.role == 1 && self.last_tag >= 6)?
			}
		}
		Ok(())
	}
	pub(super) async fn identical(
		&self,
		io: &Io,
		h: &OwnedFile,
		step: &UnverifiedJournalStep2,
		work: &mut u64,
	) -> StoreResult<bool> {
		let Some(prior) = self.bodies.get(&progress_key(step)) else {
			return Ok(false);
		};
		let body = step.body()?;
		require(prior.digest == sha(&body))?;
		let raw = io.read(h, prior.offset, prior.len, work).await?;
		let old = UnverifiedJournal2::decode(&raw)?;
		require(old.step.body()? == body)?;
		Ok(true)
	}
	pub(super) fn advance(
		&mut self,
		record: UnverifiedJournal2,
		raw: &[u8],
		offset: u64,
	) -> StoreResult<()> {
		let digest = sha(&record.step.body()?);
		let key = progress_key(&record.step);
		self.bodies.entry(key).or_insert(PriorBody {
			offset,
			len: raw.len(),
			digest,
		});
		match record.step {
			UnverifiedJournalStep2::Prepared(c) => self.claim = Some(c),
			UnverifiedJournalStep2::TerminalObserved { result, .. } => {
				let (_, _, _, ApplyResult2::Parent { record, .. }) = ApplyResult2::decode(&result)?
				else {
					return Err(StoreError::Unavailable);
				};
				self.terminal = record.terminal;
				self.terminal_result = Some(sha(&result));
			}
			_ => {}
		}
		self.last_tag = key.0;
		self.previous = sha(raw);
		self.count += 1;
		self.endpoint = offset + raw.len() as u64;
		Ok(())
	}
}
pub(super) async fn scan_chain(
	io: &Io,
	path: &Path,
	id: ContainerId,
	entry: &ParentEntry,
	work: &mut u64,
) -> StoreResult<ChainState> {
	let h = io.open(path, false, false).await?;
	let len = io.adapter.length(&h.handle).await?;
	if len > entry.credit.journal as u64 {
		return Err(StoreError::Limit);
	}
	let mut state = ChainState::default();
	if len < 269 {
		state.damaged = Some((0, len));
		return Ok(state);
	}
	let header = io.read(&h, 0, 269, work).await?;
	if header != journal_header(id, &entry.owner)? {
		state.damaged = Some((0, len));
		return Ok(state);
	}
	state.endpoint = 269;
	while state.endpoint < len {
		let start = state.endpoint;
		// IO failures propagate; malformed bytes alone become a chain-local suffix.
		if len - start < 16 {
			state.damaged = Some((start, len));
			break;
		}
		let prefix = io.read(&h, start, 16, work).await?;
		let n = u32::from_le_bytes(
			prefix[12..16]
				.try_into()
				.map_err(|_| StoreError::Unavailable)?,
		) as usize;
		if n > JOURNAL_MAX - 48 || n as u64 + 48 > len - start {
			state.damaged = Some((start, len));
			break;
		}
		let raw = io.read(&h, start, n + 48, work).await?;
		let record = match UnverifiedJournal2::decode(&raw) {
			Ok(r) => r,
			Err(_) => {
				state.damaged = Some((start, len));
				break;
			}
		};
		if state.count >= 4096 {
			return Err(StoreError::Limit);
		}
		if state.validate(entry, &record).is_err() {
			state.damaged = Some((start, len));
			break;
		}
		match state.identical(io, &h, &record.step, work).await {
			Ok(_) => {}
			Err(StoreError::Io(e)) => return Err(StoreError::Io(e)),
			Err(StoreError::Limit) => return Err(StoreError::Limit),
			Err(_) => {
				state.damaged = Some((start, len));
				break;
			}
		}
		state.advance(record, &raw, start)?;
	}
	if state.count == 0 {
		state.damaged = Some((269, len))
	}
	Ok(state)
}
