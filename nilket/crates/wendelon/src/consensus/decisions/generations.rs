//! ADR 0699 S5a: certified generations.
//!
//! A generation `G` covers a closed cut `T`: every body reserved at or before
//! `T` is terminal with its decision at or before `T`, every covered noop was
//! recorded at or before `T`, and every covered record's retry key is already
//! retired by the floor. Every configured member builds and durably stages the
//! positive base inventory for `G`, then reports its per-chunk counts and
//! roots. `Commit` applies only when every member reported the recorded target
//! and all reports agree; it then erases the covered records and makes `G`
//! current, with `K_min` at the commit's own index.
//!
//! Only the current certificate and one pending target with its reports are
//! retained. Generations advance by their recorded predecessor, never by gid
//! arithmetic, and the gid counter is monotonic, so a late report cannot
//! recreate retired state.

use super::*;

/// Chunks per generation: the high byte of a ring position.
pub const GENERATION_CHUNKS: usize = 256;
/// The survivor rule this mode certifies: newest Accepted version per key,
/// winning tombstones kept.
pub const SURVIVOR_POLICY: u8 = 1;
pub const CHUNK_BITS: u8 = 8;

/// A proposed generation, frozen when recorded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Target {
	pub gid: u64,
	pub t: u64,
	pub predecessor_gid: u64,
	/// The configured voters at their current incarnations, in node order.
	pub members: Vec<Signer>,
	pub chunk_bits: u8,
	pub survivor_policy: u8,
}

pub fn target_digest(target: &Target) -> [u8; 32] {
	let bytes = postcard::to_stdvec(target).expect("a target encodes");
	let mut hasher = blake3::Hasher::new_derive_key("wendelon generation target v1");
	hasher.update(&bytes);
	*hasher.finalize().as_bytes()
}

/// One chunk's certified survivors: how many, and their accumulator root.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkSum {
	pub count: u64,
	pub root: [u8; 32],
}

/// A member's report of its durably staged inventory for one target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportBody {
	pub gid: u64,
	pub target_digest: [u8; 32],
	pub reporter: Signer,
	/// Exactly [`GENERATION_CHUNKS`] entries, empty chunks included.
	pub chunks: Vec<ChunkSum>,
}

/// The current generation's certificate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Certificate {
	pub gid: u64,
	pub target_digest: [u8; 32],
	pub t: u64,
	pub predecessor_gid: u64,
	pub chunks: Vec<ChunkSum>,
	/// The Commit's log index: `K_min`.
	pub committed_at: u64,
}

/// The pending target and the reports recorded for it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pending {
	pub target: Target,
	pub digest: [u8; 32],
	pub recorded_at: u64,
	pub reports: BTreeMap<u32, ReportBody>,
}

/// Generation state: bounded to the current certificate and one pending target.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Generations {
	/// `None` until the first Commit: the genesis base is empty, gid 0.
	pub current: Option<Certificate>,
	pub pending: Option<Pending>,
	/// The next gid a Target may take; never reused.
	pub next_gid: u64,
}

impl Default for Generations {
	fn default() -> Self {
		Self {
			current: None,
			pending: None,
			next_gid: 1,
		}
	}
}

impl Generations {
	pub fn current_gid(&self) -> u64 {
		self.current.as_ref().map_or(0, |cert| cert.gid)
	}
	/// Reads below this cut no longer see erased decisions.
	pub fn k_min(&self) -> u64 {
		self.current.as_ref().map_or(0, |cert| cert.committed_at)
	}
	/// Every body reserved at or before this was erased.
	pub fn fr(&self) -> u64 {
		self.current.as_ref().map_or(0, |cert| cert.t)
	}
}

/// The configured voters of the last epoch at their current incarnations.
pub(crate) fn current_members(state: &SmState) -> Option<Vec<Signer>> {
	let (_, config) = state.epoch_history.last_key_value()?;
	let mut members: Vec<Signer> = config
		.peers
		.values()
		.map(|peer| {
			current_incarnation(state, peer.node_id.id).map(|incarnation| Signer {
				node: peer.node_id.id,
				incarnation,
			})
		})
		.collect::<Option<_>>()?;
	members.sort();
	members.dedup();
	Some(members)
}

/// The closed set `T` covers: bodies and noops, each terminal at or before
/// `T` and with its retry key already retired. `None` when `T` is not closed.
pub(crate) fn covered(decisions: &DecisionState, t: u64) -> Option<(Vec<u64>, Vec<u64>)> {
	let mut bodies = Vec::new();
	for (rsv, body) in decisions.bodies.range(..=t) {
		match body.status.dec() {
			Some(dec) if dec <= t && body.status.is_terminal() => {}
			_ => return None,
		}
		if decisions.keys.contains_key(&body.claim.key) {
			return None;
		}
		bodies.push(*rsv);
	}
	let mut noops = Vec::new();
	for (dec, noop) in decisions.noops.range(..=t) {
		if decisions.keys.contains_key(&noop.body.key) {
			return None;
		}
		noops.push(*dec);
	}
	Some((bodies, noops))
}

pub(super) fn propose_target(
	state: &SmState,
	decisions: &mut DecisionState,
	target: Target,
	log_index: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	use DecisionRefusal::*;
	let generations = &decisions.generations;
	if target.gid != generations.next_gid
		|| target.predecessor_gid != generations.current_gid()
		|| target.t >= log_index
		|| target.t <= generations.fr()
		|| target.chunk_bits != CHUNK_BITS
		|| target.survivor_policy != SURVIVOR_POLICY
		|| current_members(state).as_ref() != Some(&target.members)
	{
		return Err(StaleGeneration);
	}
	if covered(decisions, target.t).is_none() {
		return Err(TargetNotAdmissible);
	}
	let digest = target_digest(&target);
	let gid = target.gid;
	// A new target supersedes an uncommitted one; its reports are refused hereafter.
	decisions.generations.pending = Some(Pending {
		target,
		digest,
		recorded_at: log_index,
		reports: BTreeMap::new(),
	});
	decisions.generations.next_gid = gid + 1;
	Ok(DecisionResult::TargetRecorded { gid })
}

pub(super) fn record_report(
	state: &SmState,
	decisions: &mut DecisionState,
	signed: &Signed<ReportBody>,
) -> Result<DecisionResult, DecisionRefusal> {
	use DecisionRefusal::*;
	let verified = evidence::verify(decisions, signed)?;
	let report = verified.body();
	let Some(pending) = decisions.generations.pending.as_ref() else {
		return Err(StaleGeneration);
	};
	if report.gid != pending.target.gid || report.target_digest != pending.digest {
		return Err(StaleGeneration);
	}
	if report.chunks.len() != GENERATION_CHUNKS
		|| !pending.target.members.contains(&report.reporter)
		|| current_incarnation(state, report.reporter.node) != Some(report.reporter.incarnation)
	{
		return Err(ReportMismatch);
	}
	match pending.reports.get(&report.reporter.node) {
		Some(existing) if existing == report => {}
		Some(_) => return Err(ReportMismatch),
		None => {
			let report = report.clone();
			let pending = decisions
				.generations
				.pending
				.as_mut()
				.expect("checked above");
			pending.reports.insert(report.reporter.node, report);
		}
	}
	let pending = decisions.generations.pending.as_ref().expect("present");
	Ok(DecisionResult::ReportRecorded {
		gid: pending.target.gid,
		reports: pending.reports.len() as u32,
	})
}

pub(super) fn commit_generation(
	state: &SmState,
	decisions: &mut DecisionState,
	gid: u64,
	log_index: u64,
) -> Result<DecisionResult, DecisionRefusal> {
	use DecisionRefusal::*;
	let Some(pending) = decisions.generations.pending.as_ref() else {
		return Err(StaleGeneration);
	};
	if pending.target.gid != gid
		|| pending.target.predecessor_gid != decisions.generations.current_gid()
		|| current_members(state).as_ref() != Some(&pending.target.members)
	{
		return Err(StaleGeneration);
	}
	// Every member reported, and every report agrees chunk by chunk.
	let mut reports = pending.target.members.iter().map(|member| {
		pending
			.reports
			.get(&member.node)
			.filter(|report| report.reporter == *member)
	});
	let Some(Some(first)) = reports.next() else {
		return Err(BarrierIncomplete);
	};
	let chunks = first.chunks.clone();
	for report in reports {
		match report {
			Some(report) if report.chunks == chunks => {}
			Some(_) => return Err(ReportMismatch),
			None => return Err(BarrierIncomplete),
		}
	}
	// The recorded target, rechecked: its cut is still closed.
	let t = pending.target.t;
	let Some((bodies, noops)) = covered(decisions, t) else {
		return Err(TargetNotAdmissible);
	};
	let certificate = Certificate {
		gid,
		target_digest: pending.digest,
		t,
		predecessor_gid: pending.target.predecessor_gid,
		chunks,
		committed_at: log_index,
	};
	// The bytes the cut frees, summed and subtracted with checks before anything
	// is removed, so this refusal, like every other, leaves the state unchanged.
	let freed = bodies
		.iter()
		.map(|rsv| decisions.bodies[rsv].claim_bytes)
		.chain(noops.iter().map(|dec| decisions.noops[dec].bytes))
		.try_fold(0u64, |sum, bytes| sum.checked_add(bytes))
		.ok_or(CounterOverflow)?;
	let retained = decisions
		.retained_claim_bytes
		.checked_sub(freed)
		.ok_or(CounterOverflow)?;
	// Every check passed: commit.
	for rsv in bodies {
		decisions.bodies.remove(&rsv);
	}
	for dec in noops {
		decisions.noops.remove(&dec);
	}
	decisions.retained_claim_bytes = retained;
	decisions.generations.current = Some(certificate);
	decisions.generations.pending = None;
	Ok(DecisionResult::GenerationCommitted { gid })
}

/// The highest record at or below `t` that keeps `t` from being closed: a body
/// not terminal by `t` or whose key is not retired, or a noop whose key is not.
fn blocking_point(decisions: &DecisionState, t: u64) -> Option<u64> {
	let bodies = decisions.bodies.range(..=t).filter_map(|(rsv, body)| {
		let closed = body.status.is_terminal()
			&& body.status.dec().is_some_and(|dec| dec <= t)
			&& !decisions.keys.contains_key(&body.claim.key);
		(!closed).then_some(*rsv)
	});
	let noops = decisions
		.noops
		.range(..=t)
		.filter_map(|(dec, noop)| decisions.keys.contains_key(&noop.body.key).then_some(*dec));
	bodies.chain(noops).max()
}

/// Whether the pending target names the current voters at their current
/// incarnations. A store keeps its incarnation across restarts; a replaced
/// store (a new incarnation) or a voter change makes the target uncommittable
/// for good: its reports are refused and so is Commit.
fn pending_is_current(state: &SmState, pending: &Pending) -> bool {
	current_members(state).as_ref() == Some(&pending.target.members)
}

/// The leader's next target: the highest closed cut below the applied index,
/// above the current certificate, that erases something. `None` while a
/// current target is pending or nothing is eligible. A pending target whose
/// members are no longer current is replaced by a fresh one from the same
/// predecessor, at a fresh gid, with the current members; no voter is ever
/// dropped and a missing report never counts as agreement.
pub(crate) fn next_target(state: &SmState, applied: u64) -> Option<Target> {
	let decisions = state.decisions.as_ref()?;
	let generations = &decisions.generations;
	if generations
		.pending
		.as_ref()
		.is_some_and(|pending| pending_is_current(state, pending))
	{
		return None;
	}
	let mut t = applied.checked_sub(1)?;
	loop {
		if t <= generations.fr() {
			return None;
		}
		match blocking_point(decisions, t) {
			None => break,
			Some(point) => t = point.checked_sub(1)?,
		}
	}
	if decisions.bodies.range(..=t).next().is_none() && decisions.noops.range(..=t).next().is_none()
	{
		return None;
	}
	Some(Target {
		gid: generations.next_gid,
		t,
		predecessor_gid: generations.current_gid(),
		members: current_members(state)?,
		chunk_bits: CHUNK_BITS,
		survivor_policy: SURVIVOR_POLICY,
	})
}

/// The leader's Commit, once every member of a current pending target
/// reported. A stale target is replaced by `next_target`, never committed.
pub(crate) fn ready_commit(state: &SmState) -> Option<u64> {
	let pending = state.decisions.as_ref()?.generations.pending.as_ref()?;
	(pending_is_current(state, pending)
		&& pending
			.target
			.members
			.iter()
			.all(|member| pending.reports.contains_key(&member.node)))
	.then_some(pending.target.gid)
}

/// ADR 0699 S5a: every chunk's count and accumulator root over `(chunk,
/// preimage)` items, under the certified-base digest domain; empty chunks
/// included. Storage builds the preimages; only this computes digests.
pub fn chunk_sums_of(items: impl IntoIterator<Item = (usize, Vec<u8>)>) -> Vec<ChunkSum> {
	use crate::hash::state_digest::{
		DOMAIN_CERTIFIED_BASE, StateDigestAccumulator, StateDigestElement,
	};
	let mut accumulators: Vec<StateDigestAccumulator> = (0..GENERATION_CHUNKS)
		.map(|_| StateDigestAccumulator::new())
		.collect();
	let mut counts = vec![0u64; GENERATION_CHUNKS];
	for (chunk, preimage) in items {
		accumulators[chunk].add_element(StateDigestElement::project(
			DOMAIN_CERTIFIED_BASE,
			&preimage,
		));
		counts[chunk] += 1;
	}
	accumulators
		.into_iter()
		.zip(counts)
		.map(|(accumulator, count)| ChunkSum {
			count,
			root: accumulator.finish().into_bytes(),
		})
		.collect()
}
