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
/// Wendelon ADR 0115: policy 1, except that every (tenant, table) whose drop
/// outcome was finalized at or before the target's cut is excluded. Carried by a
/// target iff such an outcome exists; test-admitted only until enablement.
pub const SURVIVOR_POLICY_EXCLUDING: u8 = 2;
/// Wendelon ADR 0122 D2: policy 1, then every governed winner strictly below the
/// target's signed retention floor omitted. Carried iff the target's floor is
/// above 0.
pub const SURVIVOR_POLICY_AGED: u8 = 3;
/// Wendelon ADR 0122 D2: policy 2 (ADR 0115's exclusion), then the age omission.
pub const SURVIVOR_POLICY_EXCLUDING_AGED: u8 = 4;

/// Wendelon ADR 0122 D2: the signed survivor policy of a target whose cut's
/// base policy is `base` (1 or 2) and whose floor is `floor_ms`: the aged
/// component exactly when the floor is above 0.
pub fn compose_survivor_policy(base: u8, floor_ms: u64) -> u8 {
	match (base, floor_ms > 0) {
		(SURVIVOR_POLICY_EXCLUDING, true) => SURVIVOR_POLICY_EXCLUDING_AGED,
		(_, true) => SURVIVOR_POLICY_AGED,
		(base, false) => base,
	}
}

/// Wendelon ADR 0122 D2: whether a policy excludes ADR 0115's dropped tables.
pub fn policy_excludes_drops(policy: u8) -> bool {
	matches!(
		policy,
		SURVIVOR_POLICY_EXCLUDING | SURVIVOR_POLICY_EXCLUDING_AGED
	)
}

/// Wendelon ADR 0122 D2: whether a policy omits aged governed winners.
pub fn policy_omits_aged(policy: u8) -> bool {
	matches!(
		policy,
		SURVIVOR_POLICY_AGED | SURVIVOR_POLICY_EXCLUDING_AGED
	)
}
pub const CHUNK_BITS: u8 = 8;

/// Wendelon ADR 0121 D3: the least advance of a retention floor that an
/// age-only target may propose: `max(1 s, window / 4)`.
pub fn retention_step_ms(window_ms: u64) -> u64 {
	(window_ms / 4).max(1_000)
}

/// Wendelon ADR 0121 D2: the retention cut a target freezes. `floor_ms` bounds
/// which served row versions of a governed table are visible under the
/// generation: a version whose HLC is strictly below
/// `HybridTimestamp { physical_ms: floor_ms, logical: 0, node_id: 0 }` is
/// hidden. It is never recomputed from a clock after the target's apply.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionCut {
	/// The store's bound window.
	pub window_ms: u64,
	/// The leader's gated stamp; 0 for a target that needs no clock sample.
	pub stamp_ms: u64,
	pub floor_ms: u64,
}

impl RetentionCut {
	/// The floor as an HLC bound: hidden iff strictly below it.
	pub fn floor_hlc(&self) -> Option<crate::schema::HybridTimestamp> {
		(self.floor_ms > 0).then_some(crate::schema::HybridTimestamp {
			physical_ms: self.floor_ms,
			logical: 0,
			node_id: 0,
		})
	}
}

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
	/// Wendelon ADR 0121 D2.
	pub retention: RetentionCut,
}

pub fn target_digest(target: &Target) -> [u8; 32] {
	let bytes = postcard::to_stdvec(target).expect("a target encodes");
	let mut hasher = blake3::Hasher::new_derive_key("wendelon generation target v2");
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
	/// Wendelon ADR 0121 D3: the least HLC of the staged candidate's governed,
	/// live base entries at or above the target's floor; `None` when there is
	/// none. Computed while staging and stored with the staged candidate.
	pub oldest_visible: Option<crate::schema::HybridTimestamp>,
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
	/// Wendelon ADR 0121 D2: the target's retention cut, frozen.
	pub retention: RetentionCut,
	/// Wendelon ADR 0121 D3: the agreed least visible governed HLC.
	pub oldest_visible: Option<crate::schema::HybridTimestamp>,
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
	/// Wendelon ADR 0123 D7: the settlement watermark, monotone: the largest
	/// retention stamp of any committed SAMPLED target (0 at genesis). An unsampled
	/// commit and a replaced target leave it. Replicated, so a restart, a replay, an
	/// install and a leader change all read the same value.
	pub settled_ms: u64,
}

impl Default for Generations {
	fn default() -> Self {
		Self {
			current: None,
			pending: None,
			next_gid: 1,
			settled_ms: 0,
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
	/// Wendelon ADR 0121: the current certificate's retention floor (ms); 0
	/// before the first commit or with no window.
	pub fn floor_ms(&self) -> u64 {
		self.current
			.as_ref()
			.map_or(0, |cert| cert.retention.floor_ms)
	}
	/// Wendelon ADR 0121 D2: the floor a statement validated against this
	/// generation state hides strictly below.
	pub fn retention_floor(&self) -> Option<crate::schema::HybridTimestamp> {
		self.current
			.as_ref()
			.and_then(|cert| cert.retention.floor_hlc())
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

/// Wendelon ADR 0123 D2: whether a live retry key keeps its record from being
/// covered. On a positive-window store it does not (the key keeps a settled
/// receipt); at window 0 it does, as before.
pub(crate) fn live_key_blocks(decisions: &DecisionState, key: &ParentKey) -> bool {
	decisions.retention_window_ms() == 0 && decisions.keys.contains_key(key)
}

/// The closed set `T` covers: bodies and noops, each terminal at or before
/// `T`, and (at window 0) with its retry key already retired. `None` when `T`
/// is not closed.
pub(crate) fn covered(decisions: &DecisionState, t: u64) -> Option<(Vec<u64>, Vec<u64>)> {
	let mut bodies = Vec::new();
	for (rsv, body) in decisions.bodies.range(..=t) {
		match body.status.dec() {
			Some(dec) if dec <= t && body.status.is_terminal() => {}
			_ => return None,
		}
		if live_key_blocks(decisions, &body.claim.key) {
			return None;
		}
		bodies.push(*rsv);
	}
	let mut noops = Vec::new();
	for (dec, noop) in decisions.noops.range(..=t) {
		if live_key_blocks(decisions, &noop.body.key) {
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
		|| target.survivor_policy
			!= compose_survivor_policy(
				state.survivor_policy_with(&decisions.ctas, target.t),
				target.retention.floor_ms,
			) || current_members(state).as_ref() != Some(&target.members)
	{
		return Err(StaleGeneration);
	}
	if covered(decisions, target.t).is_none() {
		return Err(TargetNotAdmissible);
	}
	// Wendelon ADR 0121 D2: the retention cut, judged on the committed clock
	// with the target's own stamp; nothing changes unless every check passes.
	let clock = decisions.clock_ms.max(target.retention.stamp_ms);
	if !retention_cut_admissible(state, decisions, &target, clock) {
		return Err(RetentionCutInadmissible);
	}
	let digest = target_digest(&target);
	let gid = target.gid;
	decisions.clock_ms = clock;
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
	// Wendelon ADR 0121 D3: the least visible governed row is agreed like a chunk.
	let oldest_visible = first.oldest_visible;
	for report in reports {
		match report {
			Some(report) if report.chunks == chunks && report.oldest_visible == oldest_visible => {}
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
		retention: pending.target.retention,
		oldest_visible,
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
	// Every check passed: commit. Wendelon ADR 0123 D1: as each covered record is
	// removed, a key still naming it (only on a positive-window store) keeps its
	// terminal outcome in place, in this same apply; a key that retired gets no
	// receipt. The key's charge was taken at admission: nothing is allocated here.
	for rsv in bodies {
		let body = decisions.bodies.remove(&rsv).expect("a covered body");
		if let Some(entry) = decisions.keys.get_mut(&body.claim.key)
			&& entry.record == RecordKind::Body
			&& entry.reservation == rsv
			&& let Some(settlement) = Settlement::of_body(&body)
		{
			entry.record = RecordKind::Settled(settlement);
		}
	}
	for dec in noops {
		let noop = decisions.noops.remove(&dec).expect("a covered noop");
		if let Some(entry) = decisions.keys.get_mut(&noop.body.key)
			&& entry.record == RecordKind::Noop
			&& entry.reservation == dec
		{
			entry.record = RecordKind::Settled(Settlement::of_noop(&noop));
		}
	}
	decisions.retained_claim_bytes = retained;
	// Wendelon ADR 0123 D7: a sampled commit advances the settlement watermark.
	let stamp_ms = certificate.retention.stamp_ms;
	if stamp_ms > 0 {
		decisions.generations.settled_ms = decisions.generations.settled_ms.max(stamp_ms);
	}
	decisions.generations.current = Some(certificate);
	decisions.generations.pending = None;
	Ok(DecisionResult::GenerationCommitted { gid })
}

/// The highest record at or below `t` that keeps `t` from being closed: a body
/// not terminal by `t` or whose key is not retired, or a noop whose key is not.
/// Test-only (Codex 01a20253): the blocking point below cut `t`, as `next_target` reads it.
#[cfg(test)]
pub(crate) fn blocking_point_for_test(state: &SmState, t: u64) -> Option<u64> {
	blocking_point(state.decisions.as_ref()?, t)
}

fn blocking_point(decisions: &DecisionState, t: u64) -> Option<u64> {
	let bodies = decisions.bodies.range(..=t).filter_map(|(rsv, body)| {
		let closed = body.status.is_terminal()
			&& body.status.dec().is_some_and(|dec| dec <= t)
			&& !live_key_blocks(decisions, &body.claim.key);
		(!closed).then_some(*rsv)
	});
	let noops = decisions
		.noops
		.range(..=t)
		.filter_map(|(dec, noop)| live_key_blocks(decisions, &noop.body.key).then_some(*dec));
	bodies.chain(noops).max()
}

/// Whether the pending target names the current voters at their current
/// incarnations. A store keeps its incarnation across restarts; a replaced
/// store (a new incarnation) or a voter change makes the target uncommittable
/// for good: its reports are refused and so is Commit.
fn pending_is_current(state: &SmState, pending: &Pending) -> bool {
	current_members(state).as_ref() == Some(&pending.target.members)
}

/// Whether a generation at cut `t` erases something: a covered body or noop,
/// (ADR 0115) a finalized drop no certificate excludes yet, or (wendelon ADR
/// 0120) a retired CTAS object.
fn erases_something(state: &SmState, decisions: &DecisionState, t: u64) -> bool {
	let generations = &decisions.generations;
	decisions.bodies.range(..=t).next().is_some()
		|| decisions.noops.range(..=t).next().is_some()
		|| state.finalized_drop_in(generations.fr(), t)
		|| decisions.ctas.retired_in(generations.fr(), t)
}

/// Wendelon ADR 0123 D7 (T0): whether a record at or below `t` has a retired key.
fn retired_key_erasure(decisions: &DecisionState, t: u64) -> bool {
	decisions
		.bodies
		.range(..=t)
		.any(|(_, body)| !decisions.keys.contains_key(&body.claim.key))
		|| decisions
			.noops
			.range(..=t)
			.any(|(_, noop)| !decisions.keys.contains_key(&noop.body.key))
}

/// Wendelon ADR 0121 D2, D3: whether `target`'s retention cut may be recorded
/// on the committed clock `clock` (the applied clock raised by the target's
/// own stamp). The window must be the store's binding; the floor never falls
/// below the current certificate's nor rises above `clock - window`; with no
/// window it is 0. A target that erases nothing and raises the floor is an
/// age-only target: it must hide at least the agreed least visible row.
fn retention_cut_admissible(
	state: &SmState,
	decisions: &DecisionState,
	target: &Target,
	clock: u64,
) -> bool {
	let cut = target.retention;
	let Some(binding) = decisions.retention else {
		return false;
	};
	if cut.window_ms != binding.window_ms {
		return false;
	}
	if binding.window_ms == 0 {
		return cut.floor_ms == 0;
	}
	let current = decisions.generations.floor_ms();
	if cut.floor_ms < current || cut.floor_ms > clock.saturating_sub(binding.window_ms) {
		return false;
	}
	if cut.floor_ms > current && !erases_something(state, decisions, target.t) {
		let oldest = decisions
			.generations
			.current
			.as_ref()
			.and_then(|cert| cert.oldest_visible);
		return oldest.is_some_and(|oldest| cut.floor_ms > oldest.physical_ms);
	}
	true
}

/// Wendelon ADR 0121 D2: the cut a leader puts on a target. `now_ms`, the
/// leader's gated decision clock, is used only to raise the floor; a target
/// without it needs no clock sample and keeps the certificate's floor.
fn retention_cut_for(decisions: &DecisionState, now_ms: Option<u64>) -> Option<RetentionCut> {
	let window_ms = decisions.retention?.window_ms;
	let current = decisions.generations.floor_ms();
	Some(match now_ms {
		Some(now) if window_ms > 0 => RetentionCut {
			window_ms,
			stamp_ms: now,
			floor_ms: current.max(now.saturating_sub(window_ms)),
		},
		_ => RetentionCut {
			window_ms,
			stamp_ms: 0,
			floor_ms: current,
		},
	})
}

/// Wendelon ADR 0121 D3: whether `now_ms` lets a positive window's floor advance by at least a
/// step past the certificate's.
fn step_reached(decisions: &DecisionState, now_ms: u64) -> bool {
	let window_ms = decisions.retention_window_ms();
	window_ms > 0
		&& now_ms.saturating_sub(window_ms)
			>= decisions
				.generations
				.floor_ms()
				.saturating_add(retention_step_ms(window_ms))
}

/// Wendelon ADR 0121 D3: whether an age-only target is due at `now_ms`: a
/// positive window, an agreed least visible governed row older than the
/// window, and a floor advance of at least one step.
fn age_due(decisions: &DecisionState, now_ms: u64) -> bool {
	let Some(binding) = decisions.retention else {
		return false;
	};
	if binding.window_ms == 0 {
		return false;
	}
	let Some(cert) = decisions.generations.current.as_ref() else {
		return false;
	};
	let Some(oldest) = cert.oldest_visible else {
		return false;
	};
	let candidate = now_ms.saturating_sub(binding.window_ms);
	oldest.physical_ms < candidate
		&& candidate
			>= cert
				.retention
				.floor_ms
				.saturating_add(retention_step_ms(binding.window_ms))
}

/// The leader's next target: the highest closed cut below the applied index,
/// above the current certificate, that erases something or (wendelon ADR 0121
/// D3) whose floor is due by age at `now_ms`. `None` while a current target is
/// pending or nothing is eligible. A pending target whose members are no
/// longer current is replaced by a fresh one from the same predecessor, at a
/// fresh gid, with the current members; no voter is ever dropped and a missing
/// report never counts as agreement. `now_ms` is the leader's gated decision
/// clock, or `None` when it has no fresh sample: an ordinary target never
/// needs one.
pub(crate) fn next_target(state: &SmState, applied: u64, now_ms: Option<u64>) -> Option<Target> {
	next_target_with_reason(state, applied, now_ms).map(|(target, _)| target)
}

/// Wendelon ADR 0123 D7: why the proposer proposed a target, named once even
/// when several reasons hold, in this order. Proposer policy only: a target does
/// not carry it, and apply enforces none of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TargetReason {
	/// T1: a finalized drop or a retired CTAS object no certificate excludes yet.
	Structure,
	/// T2: a positive window's floor due by age.
	Age,
	/// T3: retained claim bytes at the pressure mark, with enough to release.
	Pressure,
	/// T4: a live key's record old enough to settle, the watermark an interval old.
	Settlement,
	/// T0: a covered record whose key has retired (today's ordinary trigger).
	RetiredKey,
}

impl TargetReason {
	pub(crate) fn label(self) -> &'static str {
		match self {
			Self::Structure => "structure",
			Self::Age => "age",
			Self::Pressure => "pressure",
			Self::Settlement => "settlement",
			Self::RetiredKey => "retired_key",
		}
	}
}

/// Wendelon ADR 0123 D7: the settlement interval S, provisional (a hypothesis the
/// controls measure; no cadence is claimed from it).
pub(crate) const SETTLE_INTERVAL_MS: u64 = 60_000;

/// Wendelon ADR 0123 D7: the claim-pressure mark P and the least release R that
/// makes a pressure target worth its rebuild, provisional.
pub(crate) const PRESSURE_MARK_BYTES: u64 = super::MAX_RETAINED_CLAIM_BYTES / 2;
pub(crate) const PRESSURE_RELEASE_BYTES: u64 = super::MAX_RETAINED_CLAIM_BYTES / 8;

/// Wendelon ADR 0123 D7 (T3): pressure is due when the retained claim bytes reach
/// the mark and the cut would release at least the least release; a tiny
/// releasable prefix never proposes, so pressure held by records no generation
/// can release is left to admission's `RetainedFull`.
pub(crate) fn pressure_due(retained: u64, releasable: u64) -> bool {
	retained >= PRESSURE_MARK_BYTES && releasable >= PRESSURE_RELEASE_BYTES
}

/// P: the production mark, or (test-only, wendelon ADR 0123 L3) the fixture's, set
/// on every voter.
pub(crate) fn pressure_mark_bytes(decisions: &DecisionState) -> u64 {
	#[cfg(test)]
	if let Some((_, mark, _)) = decisions.pressure_fixture {
		return mark;
	}
	let _ = decisions;
	PRESSURE_MARK_BYTES
}

/// R: the production least release, or (test-only) the fixture's.
pub(crate) fn pressure_release_bytes(decisions: &DecisionState) -> u64 {
	#[cfg(test)]
	if let Some((_, _, release)) = decisions.pressure_fixture {
		return release;
	}
	let _ = decisions;
	PRESSURE_RELEASE_BYTES
}

/// Wendelon ADR 0123 D7 (T4): a settlement is due at `now` when the watermark is
/// an interval old and some record at or below `t` keeps a live key issued at
/// least an interval before `now`.
fn settlement_due(decisions: &DecisionState, t: u64, now: u64) -> bool {
	let Some(old_enough) = now.checked_sub(SETTLE_INTERVAL_MS) else {
		return false;
	};
	if decisions
		.generations
		.settled_ms
		.saturating_add(SETTLE_INTERVAL_MS)
		> now
	{
		return false;
	}
	let live_old = |key: &ParentKey| {
		decisions.keys.contains_key(key)
			&& key.issued_ms().is_some_and(|issued| issued <= old_enough)
	};
	decisions
		.bodies
		.range(..=t)
		.any(|(_, body)| live_old(&body.claim.key))
		|| decisions
			.noops
			.range(..=t)
			.any(|(_, noop)| live_old(&noop.body.key))
}

/// Whether the records at or below `t` hold at least `least` claim bytes; the walk
/// stops as soon as they do.
fn releases_at_least(decisions: &DecisionState, t: u64, least: u64) -> bool {
	let mut sum = 0u64;
	decisions
		.bodies
		.range(..=t)
		.map(|(_, body)| body.claim_bytes)
		.chain(decisions.noops.range(..=t).map(|(_, noop)| noop.bytes))
		.any(|bytes| {
			sum = sum.saturating_add(bytes);
			sum >= least
		})
}

/// [`next_target`] with the reason the proposer names it by.
pub(crate) fn next_target_with_reason(
	state: &SmState,
	applied: u64,
	now_ms: Option<u64>,
) -> Option<(Target, TargetReason)> {
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
	// Something to erase, or (ADR 0115) a finalized drop no certificate excludes yet. (Wendelon ADR
	// 0120: or a retired CTAS object.) Wendelon ADR 0121 D3: or a floor due by age, which the
	// target's own stamp advances, so an idle store needs no write.
	// Wendelon ADR 0123 D7: what the cut may cover (D2) is separate from what starts
	// a generation. T0, a covered record whose key has retired (at window 0 that is
	// every covered record, as before), or T1, structure. A fresh terminal record
	// whose key is live starts nothing; a target started for another reason still
	// covers and settles it.
	let generations = &decisions.generations;
	let structure = state.finalized_drop_in(generations.fr(), t)
		|| decisions.ctas.retired_in(generations.fr(), t);
	let retired = retired_key_erasure(decisions, t);
	let erases = retired || structure;
	let due = now_ms.is_some_and(|now| age_due(decisions, now));
	// Wendelon ADR 0123 D7: T3 and T4 only on a positive window and only with a fresh
	// sample (with none, T0 and T1 continue unsampled, as today).
	let positive = decisions.retention_window_ms() > 0;
	// The cheap retained-byte mark first; only past it are the covered records walked,
	// and only until the least release is reached.
	let pressure = positive
		&& now_ms.is_some()
		&& decisions.retained_claim_bytes >= pressure_mark_bytes(decisions)
		&& releases_at_least(decisions, t, pressure_release_bytes(decisions));
	let settlement = positive && now_ms.is_some_and(|now| settlement_due(decisions, t, now));
	let reason = if structure {
		TargetReason::Structure
	} else if due {
		TargetReason::Age
	} else if pressure {
		TargetReason::Pressure
	} else if settlement {
		TargetReason::Settlement
	} else if retired {
		TargetReason::RetiredKey
	} else {
		return None;
	};
	// A target that erases something raises the floor too whenever a sample lets it advance by
	// a step: it commits anyway, so visibility follows the window at no extra generation. With
	// no sample it keeps the certificate's floor and carries no stamp. Wendelon ADR 0123 D7: a
	// pressure or settlement target always carries its fresh sample, even when the age step is
	// not due, so its commit advances the settlement watermark.
	let raise = due
		|| pressure
		|| settlement
		|| (erases && now_ms.is_some_and(|now| step_reached(decisions, now)));
	let retention = retention_cut_for(decisions, if raise { now_ms } else { None })?;
	let target = Target {
		gid: generations.next_gid,
		t,
		predecessor_gid: generations.current_gid(),
		members: current_members(state)?,
		chunk_bits: CHUNK_BITS,
		// Wendelon ADR 0122 D2: the age omission is a signed policy component.
		survivor_policy: compose_survivor_policy(
			state.survivor_policy_with(&decisions.ctas, t),
			retention.floor_ms,
		),
		retention,
	};
	Some((target, reason))
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
	let mut sums = ChunkAccumulator::new();
	for (chunk, preimage) in items {
		sums.add(chunk, &preimage);
	}
	sums.finish()
}

/// ADR 0106 (wendelon): `chunk_sums_of` fed one item at a time, so a caller
/// streaming a base page by page holds only the 256 accumulators.
pub struct ChunkAccumulator {
	accumulators: Vec<crate::hash::state_digest::StateDigestAccumulator>,
	counts: Vec<u64>,
}

impl ChunkAccumulator {
	pub fn new() -> Self {
		Self {
			accumulators: (0..GENERATION_CHUNKS)
				.map(|_| crate::hash::state_digest::StateDigestAccumulator::new())
				.collect(),
			counts: vec![0; GENERATION_CHUNKS],
		}
	}

	pub fn add(&mut self, chunk: usize, preimage: &[u8]) {
		use crate::hash::state_digest::{DOMAIN_CERTIFIED_BASE, StateDigestElement};
		self.accumulators[chunk]
			.add_element(StateDigestElement::project(DOMAIN_CERTIFIED_BASE, preimage));
		self.counts[chunk] += 1;
	}

	pub fn finish(self) -> Vec<ChunkSum> {
		self.accumulators
			.into_iter()
			.zip(self.counts)
			.map(|(accumulator, count)| ChunkSum {
				count,
				root: accumulator.finish().into_bytes(),
			})
			.collect()
	}
}

impl Default for ChunkAccumulator {
	fn default() -> Self {
		Self::new()
	}
}
