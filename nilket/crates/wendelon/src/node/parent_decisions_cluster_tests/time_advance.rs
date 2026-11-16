//! DST stage 1 (Codex 01a1e607, 01a1e616, 01a1e61e): the wall-time advancer for an in-process
//! ParentDecisions cluster on one [`TestTime`]. Production E, lease TTL and skew bound are
//! unchanged; only the cluster's shared wall clock moves forward, one bounded step at a time,
//! and each step is restored by evidence the cluster itself produced before the next:
//!
//! - **Before the step**, every voter that no current Full owner refreshes (a ControlOnly
//!   replacement) has its sample at the leader observed past freshness (`age >
//!   CLOCK_SAMPLE_FRESHNESS`, the gate's own rule), while the healthy clock still renews leases.
//! - **The step** is no larger than half the TTL and leaves every live Full lease `margin`
//!   (renew interval + 500 ms) of wall life.
//! - **After it**, each Full follower without a post-step sample sends the ordinary
//!   authenticated CUT (the test-directed sampling entry); each current Full lifetime's
//!   maintenance owner, then its lease owner, answers a fresh ticket.
//! - **Restoration**, all observed together at the same leader, no later than the step's one
//!   real deadline (clamped by the lease slack and the overall deadline):
//!   - every other voter's fresh sample has `remote_ms >= W`, at least `floor(n/2)` of them,
//!     and the leader's `clock_gate` passes;
//!   - the applied decision clock is `>= W` (a stamped command committed after the step);
//!   - every live Full lease has an applied renewal witness: a grant in an `AcquireLeases`
//!     entry past the pre-step applied index whose leader stamp is `>= W`, for the expected
//!     holder, incarnation, generation and range, continuing the pre-step lease, equal to the
//!     applied lease, expiring past `W + margin`;
//!   - no lineage of a Full holder changed (no takeover, regrant or new generation);
//!   - the cluster's abort latch is empty and every Full maintenance owner is running.
//!
//! A pass ticket alone is evidence of nothing here. Nothing edits a cache, a sample or a floor.

use super::*;
use crate::consensus::decisions::{Certificate, DecisionCommand, Lineage, Signer, Target};
use crate::network::decision_proposals::{
	CLOCK_SAMPLE_FRESHNESS, CutResponse, clock_gate, current_voters,
};
use crate::node::test_time::TestTime;
use crate::schema::LeaseDescriptor;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tokio::time::Instant;

/// One step's whole real budget: an owner-less voter's age-out (at most the freshness bound)
/// plus restoration, which the lease slack bounds further.
pub(super) const STEP_BOUND: Duration = Duration::from_secs(10);

/// The leader's applied view, from one state read.
#[derive(Clone, Debug)]
pub(super) struct View {
	pub(super) leader: u32,
	pub(super) applied: u64,
	pub(super) voters: BTreeSet<u32>,
	pub(super) leases: BTreeMap<u64, LeaseDescriptor>,
	pub(super) lineages: BTreeMap<u64, Lineage>,
	pub(super) clock_ms: u64,
}

/// What a run of steps did, for the record.
#[derive(Clone, Debug, Default)]
pub(super) struct Record {
	pub(super) steps: u64,
	pub(super) wall_advanced_ms: u64,
	pub(super) directed_samples: u64,
	pub(super) age_out_waits: u64,
	pub(super) sizing_waits: u64,
	pub(super) repeated_passes: u64,
	/// Applied renewal witnesses required and found, over all steps.
	pub(super) renewals: u64,
	/// The wall clock's whole movement over the run: the explicit steps (`wall_advanced_ms`)
	/// plus the real time that passed meanwhile, kept apart.
	pub(super) wall_span_ms: u64,
	/// The leader's applied log has been scanned for generation events through this index.
	pub(super) scanned_through: u64,
	/// Every generation event applied since the scan began: the event, the steps completed
	/// when it was seen, and the real time since the record's first scan.
	pub(super) generations: Vec<(GenerationEvent, u64, Duration)>,
	/// When this record's first scan ran.
	pub(super) begun: Option<Instant>,
	pub(super) restore_total: Duration,
	pub(super) restore_max: Duration,
	/// Real time per phase, over all steps: before the step (age-out and sizing), directed
	/// samples, maintenance passes, lease passes, and observation.
	pub(super) phases: [Duration; 5],
}

/// The renewal margin: the renew interval (TTL / 3) plus 500 ms.
pub(super) fn margin_ms(ttl_ms: u64) -> u64 {
	ttl_ms / 3 + 500
}

/// Voters other than the leader that no current Full owner refreshes (`full` names the slots
/// with a current Full lifetime) and whose sample is still fresh at the leader, with its age.
pub(super) fn ownerless_fresh(
	voters: &BTreeSet<u32>,
	leader: u32,
	full: &BTreeSet<u32>,
	samples: &[(u32, u64, Duration)],
) -> Vec<(u32, Duration)> {
	samples
		.iter()
		.filter(|(peer, _, age)| {
			*peer != leader
				&& voters.contains(peer)
				&& !full.contains(peer)
				&& *age <= CLOCK_SAMPLE_FRESHNESS
		})
		.map(|(peer, _, age)| (*peer, *age))
		.collect()
}

/// The next step: at most `remaining`, at most half the TTL, leaving every live Full lease
/// (`expiries`, physical ms) `margin_ms` of wall life. `None` when no step fits now.
pub(super) fn step_size(
	now_ms: u64,
	remaining: u64,
	ttl_ms: u64,
	margin_ms: u64,
	expiries: impl IntoIterator<Item = u64>,
) -> Option<u64> {
	let mut step = remaining.min(ttl_ms / 2);
	for expiry in expiries {
		step = step.min(expiry.saturating_sub(now_ms).saturating_sub(margin_ms));
	}
	(step > 0).then_some(step)
}

/// The live leases held by current Full members at `now_ms`.
pub(super) fn live_full_leases<'a>(
	view: &'a View,
	full: &'a BTreeSet<u32>,
	now_ms: u64,
) -> impl Iterator<Item = &'a LeaseDescriptor> + 'a {
	view.leases.values().filter(move |lease| {
		full.contains(&lease.holder.id) && lease.lease_expiry.physical_ms > now_ms
	})
}

/// Whether a pre-step lease must show a renewal: live, held by a Full member, under a lineage
/// of the same holder whose first grant is not pending.
fn renewal_required(
	view: &View,
	full: &BTreeSet<u32>,
	lease: &LeaseDescriptor,
	now_ms: u64,
) -> bool {
	full.contains(&lease.holder.id)
		&& lease.lease_expiry.physical_ms > now_ms
		&& view
			.lineages
			.get(&lease.range_start)
			.is_some_and(|lineage| lineage.holder == lease.holder && !lineage.first_grant_pending)
}

/// The applied renewal witness for `range_start` (see the module doc), or why there is none.
pub(super) fn renewal_witness(
	pre: &View,
	post: &View,
	entries: &[LogEntry],
	w: u64,
	margin_ms: u64,
	range_start: u64,
) -> Result<u64, String> {
	let before = pre
		.leases
		.get(&range_start)
		.ok_or_else(|| format!("range {range_start}: no pre-step lease"))?;
	let lineage = pre
		.lineages
		.get(&range_start)
		.ok_or_else(|| format!("range {range_start}: no pre-step lineage"))?;
	let after = post
		.leases
		.get(&range_start)
		.ok_or_else(|| format!("range {range_start}: no applied lease"))?;
	if post.lineages.get(&range_start) != Some(lineage) {
		return Err(format!(
			"range {range_start}: its lineage changed ({:?} -> {:?}); a takeover, regrant or new \
			 generation is no renewal",
			lineage,
			post.lineages.get(&range_start)
		));
	}
	let mut seen = Vec::new();
	for entry in entries {
		let index = entry.log_id.index;
		let openraft::EntryPayload::Normal(crate::consensus::WendelonRequest::AcquireLeases(batch)) =
			&entry.payload
		else {
			continue;
		};
		for grant in batch
			.grants()
			.iter()
			.filter(|grant| grant.lease.range_start == range_start)
		{
			let tenure = grant.lease.holder == lineage.holder
				&& grant.lease.range_end == lineage.range_end
				&& grant.lease.epoch == lineage.epoch
				&& grant.holder_incarnation == lineage.incarnation
				&& grant.expected_generation == Some(lineage.generation);
			let continues = before.lease_expiry > grant.lease.lease_start;
			let applied = grant.lease == *after;
			let accepted = index > pre.applied
				&& index <= post.applied
				&& batch.stamp_ms() >= w
				&& tenure && continues
				&& applied;
			if accepted {
				if after.lease_expiry.physical_ms <= w.saturating_add(margin_ms) {
					return Err(format!(
						"range {range_start}: renewed at index {index} but expiring at {}, not past \
						 W {w} + margin {margin_ms}",
						after.lease_expiry.physical_ms
					));
				}
				return Ok(index);
			}
			seen.push(format!(
				"index {index} stamp {} start {} (past pre {} stamp>=W {} tenure {tenure} continues \
				 {continues} applied {applied})",
				batch.stamp_ms(),
				grant.lease.lease_start.physical_ms,
				index > pre.applied && index <= post.applied,
				batch.stamp_ms() >= w,
			));
		}
	}
	Err(format!(
		"range {range_start}: no applied post-step renewal (W {w}, pre-step applied {}); grants \
		 seen: {seen:?}",
		pre.applied
	))
}

/// Everything a step's restoration still lacks (empty: restored). Pure over what was read.
#[allow(clippy::too_many_arguments)]
pub(super) fn judge(
	pre: &View,
	post: &View,
	entries: &[LogEntry],
	samples: &[(u32, u64, Duration)],
	leader_now_ms: u64,
	w: u64,
	margin_ms: u64,
	full: &BTreeSet<u32>,
	pre_now_ms: u64,
) -> Vec<String> {
	let mut missing = Vec::new();
	if post.leader != pre.leader {
		missing.push(format!(
			"leadership moved from {} to {}",
			pre.leader, post.leader
		));
	}
	let mut post_step = 0usize;
	for voter in post.voters.iter().filter(|voter| **voter != post.leader) {
		let fresh = samples
			.iter()
			.find(|(peer, _, age)| peer == voter && *age <= CLOCK_SAMPLE_FRESHNESS);
		match fresh {
			Some((_, remote, _)) if *remote >= w => post_step += 1,
			Some((_, remote, age)) => missing.push(format!(
				"voter {voter}: a fresh pre-step sample at the leader (remote {remote} < W {w}, age \
				 {age:?})"
			)),
			None => {}
		}
	}
	let needed = post.voters.len() / 2;
	if post_step < needed {
		missing.push(format!(
			"{post_step} fresh post-step voter samples, {needed} needed"
		));
	}
	if let Err(refusal) = clock_gate(leader_now_ms, post.leader, Some(&post.voters), samples) {
		missing.push(format!("the leader's clock gate: {refusal:?}"));
	}
	if post.clock_ms < w {
		missing.push(format!(
			"the applied decision clock {} is below W {w}",
			post.clock_ms
		));
	}
	for lease in pre
		.leases
		.values()
		.filter(|lease| renewal_required(pre, full, lease, pre_now_ms))
	{
		if let Err(why) = renewal_witness(pre, post, entries, w, margin_ms, lease.range_start) {
			missing.push(why);
		}
	}
	for (start, lineage) in pre
		.lineages
		.iter()
		.filter(|(_, lineage)| full.contains(&lineage.holder.id))
	{
		if post
			.lineages
			.get(start)
			.map(|after| (after.holder, after.generation))
			!= Some((lineage.holder, lineage.generation))
		{
			missing.push(format!(
				"range {start}: the lineage of Full holder {} changed to {:?}",
				lineage.holder.id,
				post.lineages.get(start)
			));
		}
	}
	missing
}

/// A step's conclusion from one observation: `Some(Ok)` restored within its deadline,
/// `Some(Err)` observed after it (even if restored: a late success is refused), `None` not yet.
pub(super) fn conclude(
	missing: &[String],
	observed_at: Instant,
	deadline: Instant,
) -> Option<Result<(), String>> {
	if observed_at > deadline {
		return Some(Err(format!(
			"observed {:?} after its deadline; still missing: {missing:?}",
			observed_at.saturating_duration_since(deadline)
		)));
	}
	missing.is_empty().then_some(Ok(()))
}

/// The leader's applied view.
pub(super) async fn view(cluster: &Cluster, leader: u32) -> View {
	let (applied, voters, leases, lineages, clock_ms) = cluster
		.node(leader)
		.state_machine
		.read_state(|state| {
			let decisions = state.decisions.as_ref().expect("a ParentDecisions state");
			(
				state.last_applied_log_id.map_or(0, |id| id.index),
				current_voters(state).unwrap_or_default(),
				state
					.leases
					.iter()
					.map(|(start, lease)| (*start, *lease))
					.collect::<BTreeMap<_, _>>(),
				decisions.lineages().clone(),
				decisions.clock_ms(),
			)
		})
		.await;
	View {
		leader,
		applied,
		voters,
		leases,
		lineages,
		clock_ms,
	}
}

/// The leader's recorded clock samples with their monotonic ages; read only.
fn samples_at(cluster: &Cluster, leader: u32) -> Vec<(u32, u64, Duration)> {
	cluster
		.node(leader)
		.data_server
		.decision_proposals_for_test()
		.clock_samples_for_test()
}

/// Requests one pass from each named owner (a fresh ticket each) and waits for all of them,
/// bounded by `deadline`.
async fn passes(
	time: &TestTime,
	members: &BTreeSet<u32>,
	which: fn(&crate::node::test_time::NodeWakes) -> &crate::node::test_time::PassWake,
	what: &str,
	deadline: Instant,
) -> Result<(), String> {
	let tickets: Vec<_> = members
		.iter()
		.map(|id| {
			let wakes = time.wakes(*id);
			let ticket = which(&wakes).request();
			(*id, wakes, ticket)
		})
		.collect();
	for (id, wakes, ticket) in tickets {
		which(&wakes)
			.completed(ticket, deadline)
			.await
			.map_err(|why| format!("node {id}'s {what} pass: {why}"))?;
	}
	Ok(())
}

/// Advances the cluster's wall clock to `target_ms` in restored steps. `full` names the slots
/// whose current lifetimes are Full nodes in `cluster` (their owners are woken); every other
/// voter is owner-less here. Ends at the first unrestored step, with its observed state.
pub(super) async fn advance_to(
	cluster: &Cluster,
	time: &TestTime,
	full: &BTreeSet<u32>,
	target_ms: u64,
	overall: Instant,
	record: &mut Record,
) -> Result<(), String> {
	let wall_start = time.clock.now_ms();
	if record.scanned_through == 0 {
		let leader = within(overall, "the leader", cluster.leader()).await?;
		record.scanned_through = within(overall, "the starting view", view(cluster, leader))
			.await?
			.applied;
	}
	let result = async {
		while time.clock.now_ms() < target_ms {
			let stepped = one_step(cluster, time, full, target_ms, overall, record)
				.await
				.map_err(|why| format!("step {}: {why}", record.steps + 1))?;
			if !stepped {
				break;
			}
		}
		Ok(())
	}
	.await;
	record.wall_span_ms += time.clock.now_ms().saturating_sub(wall_start);
	result
}

/// One generation event from the leader's applied log (Codex 01a1e648): observed, never
/// inferred, so a certificate can be tied to its target, its reports and its commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum GenerationEvent {
	Target {
		index: u64,
		target: Target,
		digest: [u8; 32],
	},
	Report {
		index: u64,
		gid: u64,
		digest: [u8; 32],
		reporter: Signer,
		signer: Signer,
	},
	Commit {
		index: u64,
		gid: u64,
	},
}

/// The generation events among `entries`, in log order.
pub(super) fn generation_events(entries: &[LogEntry]) -> Vec<GenerationEvent> {
	entries
		.iter()
		.filter_map(|entry| {
			let index = entry.log_id.index;
			// The harness's own reader of a decision entry: this module matches no decision.
			let command = decision_of(entry)?;
			match command {
				DecisionCommand::ProposeTarget { target } => Some(GenerationEvent::Target {
					index,
					target: target.clone(),
					digest: crate::consensus::decisions::generations::target_digest(target),
				}),
				DecisionCommand::Report { report } => Some(GenerationEvent::Report {
					index,
					gid: report.body.gid,
					digest: report.body.target_digest,
					reporter: report.body.reporter,
					signer: report.signer,
				}),
				DecisionCommand::CommitGeneration { gid } => {
					Some(GenerationEvent::Commit { index, gid: *gid })
				}
				_ => None,
			}
		})
		.collect()
}

/// Scans the leader's applied log from the record's mark through `through` for generation
/// events, bounded by `deadline`; the mark only moves forward, so nothing is scanned twice
/// and nothing between scans is skipped.
pub(super) async fn scan_generations(
	cluster: &Cluster,
	leader: u32,
	record: &mut Record,
	deadline: Instant,
	through: u64,
) -> Result<(), String> {
	if through <= record.scanned_through {
		return Ok(());
	}
	let entries = within(
		deadline,
		"the generation scan",
		log_entries(cluster, leader, record.scanned_through + 1..=through),
	)
	.await?;
	contiguous(
		entries.iter().map(|entry| entry.log_id.index),
		record.scanned_through,
		through,
	)?;
	let at = record.begun.get_or_insert_with(Instant::now).elapsed();
	let steps = record.steps;
	record.generations.extend(
		generation_events(&entries)
			.into_iter()
			.map(|event| (event, steps, at)),
	);
	record.scanned_through = through;
	Ok(())
}

/// Codex 01a1e655 R2: the scanned entries must be exactly `(after, through]`, in order, with
/// no hole or duplicate. A compacting log returns whatever remains of a range, so a purged
/// prefix or interior is INCOMPLETE EVIDENCE here, never silently skipped; the caller keeps
/// its mark unchanged on this error.
pub(super) fn contiguous(
	indices: impl IntoIterator<Item = u64>,
	after: u64,
	through: u64,
) -> Result<(), String> {
	let mut expected = after + 1;
	for index in indices {
		if index != expected {
			return Err(format!(
				"INCOMPLETE EVIDENCE: the scan of ({after}, {through}] found index {index} where 				 {expected} was due (purged or out of order)"
			));
		}
		expected += 1;
	}
	if expected != through + 1 {
		return Err(format!(
			"INCOMPLETE EVIDENCE: the scan of ({after}, {through}] ended before {expected}"
		));
	}
	Ok(())
}

/// Codex 01a1e655 R1: whether `current` is the certificate the pilot waits for: past `g`,
/// covering EVERY designated reservation by the inclusive cut rule, and with its EXACT commit
/// entry (gid and `committed_at`) already scanned. An intermediate certificate that does not
/// cover them, or one committed beyond the scan's mark, is not yet it.
pub(super) fn covering_certificate_ready(
	events: &[GenerationEvent],
	current: Option<&Certificate>,
	g: u64,
	designated: &[u64],
) -> bool {
	current.is_some_and(|current| {
		current.gid > g
			&& !designated.is_empty()
			&& designated.iter().all(|rsv| *rsv <= current.t)
			&& events.iter().any(|event| {
				matches!(event, GenerationEvent::Commit { index, gid }
					if *gid == current.gid && *index == current.committed_at)
			})
	})
}

/// The observed evidence that `certificate` was certified with `replacement` taking part and
/// covers the `designated` reservations (Codex 01a1e648): its target (same gid, digest and
/// cut) lists the replacement's current slot and incarnation; the replacement's own report
/// for that target was applied after the target and before the commit; the commit is the
/// certificate's `committed_at`; and every designated reservation is at or below its cut
/// (`covered` takes bodies `..=t`). Returns the indices (target, report, commit).
pub(super) fn certified_with(
	events: &[GenerationEvent],
	certificate: &Certificate,
	replacement: Signer,
	designated: &[u64],
) -> Result<(u64, u64, u64), String> {
	let gid = certificate.gid;
	let target_at = events
		.iter()
		.find_map(|event| match event {
			GenerationEvent::Target {
				index,
				target,
				digest,
			} if target.gid == gid
				&& *digest == certificate.target_digest
				&& target.t == certificate.t =>
			{
				Some((*index, target))
			}
			_ => None,
		})
		.ok_or_else(|| format!("no observed target for certificate {gid}"))?;
	if !target_at.1.members.contains(&replacement) {
		return Err(format!(
			"certificate {gid}'s target members {:?} do not list the replacement {replacement:?}",
			target_at.1.members
		));
	}
	// The commit is matched by gid AND index: an earlier refused or retried attempt for the
	// same gid is another entry.
	let commit_at = events
		.iter()
		.find_map(|event| match event {
			GenerationEvent::Commit {
				index,
				gid: committed,
			} if *committed == gid && *index == certificate.committed_at => Some(*index),
			_ => None,
		})
		.ok_or_else(|| {
			format!(
				"no observed commit of generation {gid} at its certificate's index {}",
				certificate.committed_at
			)
		})?;
	let report_at = events
		.iter()
		.find_map(|event| match event {
			GenerationEvent::Report {
				index,
				gid: reported,
				digest,
				reporter,
				signer,
			} if *reported == gid
				&& *digest == certificate.target_digest
				&& *reporter == replacement
				&& *signer == replacement
				&& *index > target_at.0
				&& *index < commit_at =>
			{
				Some(*index)
			}
			_ => None,
		})
		.ok_or_else(|| {
			format!("no report by the replacement {replacement:?} for generation {gid} between its target and commit")
		})?;
	if designated.is_empty() {
		return Err("no designated reservation to cover".to_owned());
	}
	if let Some(outside) = designated.iter().find(|rsv| **rsv > certificate.t) {
		return Err(format!(
			"reservation {outside} is past certificate {gid}'s cut {}",
			certificate.t
		));
	}
	Ok((target_at.0, report_at, commit_at))
}

/// Awaits `work` bounded by the absolute real `deadline`, and refuses a result OBSERVED after
/// it, even one that was ready at once (Codex 01a1e637 R2): `timeout_at` polls a ready future
/// before its timer, so the bound is checked again where the result is seen.
pub(super) async fn within<T>(
	deadline: Instant,
	what: &str,
	work: impl std::future::Future<Output = T>,
) -> Result<T, String> {
	let value = tokio::time::timeout_at(deadline, work)
		.await
		.map_err(|_| format!("{what}: not done by its deadline"))?;
	let observed_at = Instant::now();
	if observed_at > deadline {
		return Err(format!(
			"{what}: done, but observed {:?} after its deadline",
			observed_at.saturating_duration_since(deadline)
		));
	}
	Ok(value)
}

/// What the next step is, from one wall reading taken after every preparatory wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Plan {
	/// Real time already carried the wall clock to the target: done, with no step.
	Reached,
	Step(u64),
	/// No step fits the lease slack now.
	NoFit,
}

/// The plan at `now_ms` (Codex 01a1e637 R1): a reached target is completion, never an
/// underflowed or fabricated step.
pub(super) fn plan(
	now_ms: u64,
	target_ms: u64,
	ttl_ms: u64,
	margin_ms: u64,
	expiries: impl IntoIterator<Item = u64>,
) -> Plan {
	match target_ms
		.checked_sub(now_ms)
		.filter(|remaining| *remaining > 0)
	{
		None => Plan::Reached,
		Some(remaining) => match step_size(now_ms, remaining, ttl_ms, margin_ms, expiries) {
			Some(step) => Plan::Step(step),
			None => Plan::NoFit,
		},
	}
}

/// One step; `false` when the target was already reached (no step taken).
async fn one_step(
	cluster: &Cluster,
	time: &TestTime,
	full: &BTreeSet<u32>,
	target_ms: u64,
	overall: Instant,
	record: &mut Record,
) -> Result<bool, String> {
	let step_started = Instant::now();
	let step_deadline = overall.min(step_started + STEP_BOUND);
	let leader = within(step_deadline, "the leader", cluster.leader()).await?;
	if !full.contains(&leader) {
		return Err(format!("the leader {leader} is not a Full member here"));
	}
	let ttl_ms = cluster.configs[&leader].data_ring_lease_ttl.as_millis() as u64;
	let margin = margin_ms(ttl_ms);
	let latched = |time: &TestTime| {
		let latched = time.abort.latched();
		if latched.is_empty() {
			Ok(())
		} else {
			Err(format!("the cluster's abort latched: {latched:?}"))
		}
	};
	// Before the step: owner-less samples age out, and a step must fit the lease slack.
	let mut aged = false;
	let mut sized = false;
	let (pre, pre_now, step, lapse_at) = loop {
		latched(time)?;
		if Instant::now() > step_deadline {
			return Err("no step fitted before its deadline".to_owned());
		}
		let voters = within(step_deadline, "the pre-step view", view(cluster, leader))
			.await?
			.voters;
		let fresh = ownerless_fresh(&voters, leader, full, &samples_at(cluster, leader));
		if let Some(left) = fresh
			.iter()
			.map(|(_, age)| CLOCK_SAMPLE_FRESHNESS.saturating_sub(*age))
			.max()
		{
			aged = true;
			if Instant::now() + left > step_deadline {
				return Err(format!(
					"owner-less voter samples {fresh:?} cannot age out before the step deadline"
				));
			}
			tokio::time::sleep(left + Duration::from_millis(1)).await;
			continue;
		}
		let pre = within(step_deadline, "the pre-step view", view(cluster, leader)).await?;
		let now = time.clock.now_ms();
		if let Some(lapsed) = pre.leases.values().find(|lease| {
			full.contains(&lease.holder.id)
				&& lease.lease_expiry.physical_ms <= now
				&& pre.lineages.get(&lease.range_start).is_some_and(|lineage| {
					lineage.holder == lease.holder && !lineage.first_grant_pending
				})
		}) {
			return Err(format!(
				"a Full member's lease lapsed before the step: {lapsed:?}"
			));
		}
		let expiries: Vec<u64> = live_full_leases(&pre, full, now)
			.map(|lease| lease.lease_expiry.physical_ms)
			.collect();
		match plan(now, target_ms, ttl_ms, margin, expiries.iter().copied()) {
			Plan::Reached => return Ok(false),
			Plan::Step(step) => break (pre, now, step, expiries.iter().copied().min()),
			Plan::NoFit => {
				sized = true;
				passes(time, full, |wakes| &*wakes.leases, "lease", step_deadline).await?;
			}
		}
	};
	let mut mark = step_started;
	let mut phase = |record: &mut Record, at: usize| {
		let now = Instant::now();
		record.phases[at] += now - mark;
		mark = now;
	};
	phase(record, 0);
	record.age_out_waits += u64::from(aged);
	record.sizing_waits += u64::from(sized);
	let w = time.clock.advance(step);
	let stepped = Instant::now();
	let restore_deadline = match lapse_at {
		Some(lapse_at) => {
			step_deadline.min(stepped + Duration::from_millis(lapse_at.saturating_sub(w)))
		}
		None => step_deadline,
	};
	// Test-directed samples: each Full follower without a post-step sample at the leader.
	let samples = samples_at(cluster, leader);
	for follower in full
		.iter()
		.filter(|id| **id != leader && pre.voters.contains(id))
	{
		let post_step = samples.iter().any(|(peer, remote, age)| {
			peer == follower && *remote >= w && *age <= CLOCK_SAMPLE_FRESHNESS
		});
		if post_step {
			continue;
		}
		record.directed_samples += 1;
		let proposals = cluster
			.node(*follower)
			.data_server
			.decision_proposals_for_test();
		match tokio::time::timeout_at(restore_deadline, proposals.test_directed_sample()).await {
			Ok(CutResponse::Cut(_)) => {}
			Ok(other) => return Err(format!("node {follower}'s directed sample: {other:?}")),
			Err(_) => {
				return Err(format!(
					"node {follower}'s directed sample passed its deadline"
				));
			}
		}
	}
	phase(record, 1);
	passes(
		time,
		full,
		|wakes| &*wakes.maintenance,
		"maintenance",
		restore_deadline,
	)
	.await?;
	phase(record, 2);
	passes(
		time,
		full,
		|wakes| &*wakes.leases,
		"lease",
		restore_deadline,
	)
	.await?;
	phase(record, 3);
	let restored_through = loop {
		latched(time)?;
		for id in full {
			if cluster
				.node(*id)
				.handles
				.decision_maintenance
				.as_ref()
				.is_none_or(|task| task.is_finished())
			{
				return Err(format!("node {id}'s maintenance owner is not running"));
			}
		}
		let mut post = within(
			restore_deadline,
			"the post-step view",
			view(cluster, leader),
		)
		.await?;
		post.leader = within(restore_deadline, "the post-step leader", cluster.leader()).await?;
		let entries = if post.applied > pre.applied {
			within(
				restore_deadline,
				"the post-step log entries",
				log_entries(cluster, leader, pre.applied + 1..=post.applied),
			)
			.await?
		} else {
			Vec::new()
		};
		let samples = samples_at(cluster, leader);
		let missing = judge(
			&pre,
			&post,
			&entries,
			&samples,
			time.clock.now_ms(),
			w,
			margin,
			full,
			pre_now,
		);
		match conclude(&missing, Instant::now(), restore_deadline) {
			Some(Ok(())) => break post.applied,
			Some(Err(why)) => {
				return Err(format!(
					"W {w}, step {step} ms, {:?} after the step: {why}",
					stepped.elapsed()
				));
			}
			None => {
				// Ask again where evidence is missing; never waited on past the deadline.
				record.repeated_passes += 1;
				for id in full {
					time.wakes(*id).leases.request();
				}
				time.wakes(leader).maintenance.request();
				tokio::time::sleep(Duration::from_millis(5)).await;
			}
		}
	};
	phase(record, 4);
	// Bookkeeping after the judged restoration: every generation event applied through it.
	scan_generations(cluster, leader, record, overall, restored_through).await?;
	let restored = stepped.elapsed();
	record.renewals += pre
		.leases
		.values()
		.filter(|lease| renewal_required(&pre, full, lease, pre_now))
		.count() as u64;
	record.steps += 1;
	record.wall_advanced_ms += step;
	record.restore_total += restored;
	record.restore_max = record.restore_max.max(restored);
	Ok(true)
}

// ------------------------------------------------------------------
//  Focused protocol controls (Codex 01a1e61e)
// ------------------------------------------------------------------

#[cfg(test)]
mod controls {
	use super::*;
	use crate::consensus::decisions::{Certificate, Signer, Target};
	use crate::consensus::{LeaseBatch, LeaseBatchGrant, StoreIncarnation, WendelonRequest};
	use crate::schema::{HybridTimestamp, NodeId};
	use bytemuck::Zeroable;

	const W: u64 = 1_000_000;
	const MARGIN: u64 = 1_500;
	const HOLDER: u32 = 2;
	const RANGE: u64 = 100;

	fn at(physical_ms: u64) -> HybridTimestamp {
		HybridTimestamp {
			physical_ms,
			logical: 0,
			node_id: HOLDER,
		}
	}

	fn lease(start: u64, expiry: u64) -> LeaseDescriptor {
		let mut lease = LeaseDescriptor::zeroed();
		lease.epoch = 1;
		lease.holder = NodeId {
			id: HOLDER,
			_reserved: 0,
		};
		lease.range_start = RANGE;
		lease.range_end = RANGE + 99;
		lease.lease_start = at(start);
		lease.lease_expiry = at(expiry);
		lease
	}

	fn lineage(generation: u64) -> Lineage {
		Lineage {
			range_end: RANGE + 99,
			epoch: 1,
			holder: NodeId {
				id: HOLDER,
				_reserved: 0,
			},
			incarnation: StoreIncarnation::from_raw(7),
			generation,
			floor_ms: 0,
			protected_until_ms: 0,
			first_grant_pending: false,
		}
	}

	fn view(applied: u64, lease: LeaseDescriptor, lineage: Lineage, clock_ms: u64) -> View {
		View {
			leader: 1,
			applied,
			voters: [1, 2, 3].into_iter().collect(),
			leases: [(RANGE, lease)].into_iter().collect(),
			lineages: [(RANGE, lineage)].into_iter().collect(),
			clock_ms,
		}
	}

	fn entry(index: u64, stamp_ms: u64, lease: LeaseDescriptor, generation: u64) -> LogEntry {
		LogEntry {
			log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
			payload: openraft::EntryPayload::Normal(WendelonRequest::AcquireLeases(
				LeaseBatch::new(
					stamp_ms,
					vec![LeaseBatchGrant {
						lease,
						expected_generation: Some(generation),
						holder_incarnation: StoreIncarnation::from_raw(7),
					}],
				)
				.unwrap(),
			)),
		}
	}

	/// The age-out boundary is the gate's own: a sample aged exactly the freshness bound
	/// still counts (so it blocks a step), one a millisecond older does not; Full members and
	/// the leader never block.
	#[test]
	fn an_ownerless_sample_blocks_a_step_until_observed_past_freshness() {
		let voters: BTreeSet<u32> = [1, 2, 3].into_iter().collect();
		let full: BTreeSet<u32> = [1, 2].into_iter().collect();
		let at_bound = [
			(3, W, CLOCK_SAMPLE_FRESHNESS),
			(2, W, Duration::ZERO),
			(1, W, Duration::ZERO),
		];
		assert_eq!(
			ownerless_fresh(&voters, 1, &full, &at_bound),
			vec![(3, CLOCK_SAMPLE_FRESHNESS)]
		);
		let past = [(3, W, CLOCK_SAMPLE_FRESHNESS + Duration::from_millis(1))];
		assert!(ownerless_fresh(&voters, 1, &full, &past).is_empty());
		// The gate agrees on both sides of the boundary (it projects a sample by its age, so an
		// agreeing reading aged the bound was taken that long before `W`).
		let bound_ms = CLOCK_SAMPLE_FRESHNESS.as_millis() as u64;
		assert!(
			clock_gate(
				W,
				1,
				Some(&voters),
				&[(3, W - bound_ms, CLOCK_SAMPLE_FRESHNESS)]
			)
			.is_ok()
		);
		assert!(
			clock_gate(
				W,
				1,
				Some(&voters),
				&[(
					3,
					W - bound_ms - 1,
					CLOCK_SAMPLE_FRESHNESS + Duration::from_millis(1)
				)]
			)
			.is_err()
		);
	}

	/// A step leaves every live Full lease its margin, is at most half the TTL and the
	/// remainder, and does not happen at all without slack.
	#[test]
	fn a_step_is_sized_under_the_lease_slack_and_refused_without_it() {
		assert_eq!(
			step_size(W, 10_000, 3_000, MARGIN, [W + 3_000]),
			Some(1_500)
		);
		assert_eq!(step_size(W, 10_000, 3_000, MARGIN, [W + 2_000]), Some(500));
		assert_eq!(step_size(W, 200, 3_000, MARGIN, [W + 3_000]), Some(200));
		assert_eq!(step_size(W, 10_000, 3_000, MARGIN, [W + MARGIN]), None);
		assert_eq!(step_size(W, 10_000, 3_000, MARGIN, []), Some(1_500));
	}

	/// Codex 01a1e637 R1: a target that real time reached while the step prepared is
	/// completion with no step; no remainder underflows and no step is fabricated.
	#[test]
	fn a_target_reached_while_a_step_prepares_ends_without_a_step() {
		assert_eq!(plan(W, W, 3_000, MARGIN, [W + 3_000]), Plan::Reached);
		assert_eq!(plan(W + 7, W, 3_000, MARGIN, [W + 3_000]), Plan::Reached);
		assert_eq!(
			plan(W - 200, W, 3_000, MARGIN, [W + 3_000]),
			Plan::Step(200)
		);
		// A live lease with no more than the margin left: no step fits.
		assert_eq!(
			plan(W - 200, W, 3_000, MARGIN, [W - 200 + MARGIN]),
			Plan::NoFit
		);
	}

	/// Codex 01a1e648: the certification evidence accepts a certificate whose observed target
	/// lists the replacement at its current incarnation, whose replacement report lies between
	/// target and commit, and whose cut covers the designated reservation; it refuses each
	/// lookalike.
	#[test]
	fn the_certification_evidence_requires_the_replacements_target_report_and_cut() {
		let replacement = Signer {
			node: 42,
			incarnation: StoreIncarnation::from_raw(9),
		};
		let old = Signer {
			node: 42,
			incarnation: StoreIncarnation::from_raw(8),
		};
		let other = Signer {
			node: 41,
			incarnation: StoreIncarnation::from_raw(1),
		};
		let target = Target {
			gid: 2,
			t: 500,
			predecessor_gid: 1,
			members: vec![other, replacement],
			chunk_bits: 0,
			survivor_policy: 0,
		};
		let digest = crate::consensus::decisions::generations::target_digest(&target);
		let certificate = Certificate {
			gid: 2,
			target_digest: digest,
			t: 500,
			predecessor_gid: 1,
			chunks: Vec::new(),
			committed_at: 700,
		};
		let report = |index, reporter: Signer, digest| GenerationEvent::Report {
			index,
			gid: 2,
			digest,
			reporter,
			signer: reporter,
		};
		let events = vec![
			GenerationEvent::Target {
				index: 600,
				target: target.clone(),
				digest,
			},
			report(610, other, digest),
			report(620, replacement, digest),
			GenerationEvent::Commit { index: 700, gid: 2 },
		];
		assert_eq!(
			certified_with(&events, &certificate, replacement, &[450, 500]),
			Ok((600, 620, 700))
		);
		// The cut is inclusive; one past it is not covered.
		assert!(certified_with(&events, &certificate, replacement, &[501]).is_err());
		assert!(certified_with(&events, &certificate, replacement, &[]).is_err());
		// Another incarnation of the slot is not the replacement.
		assert!(certified_with(&events, &certificate, old, &[450]).is_err());
		// No report by the replacement, or one for another target, or after the commit.
		let without: Vec<_> = events
			.iter()
			.filter(|event| **event != report(620, replacement, digest))
			.cloned()
			.collect();
		assert!(certified_with(&without, &certificate, replacement, &[450]).is_err());
		let mut foreign = without.clone();
		foreign.push(report(630, replacement, [7; 32]));
		assert!(certified_with(&foreign, &certificate, replacement, &[450]).is_err());
		let mut late = without.clone();
		late.push(report(710, replacement, digest));
		assert!(certified_with(&late, &certificate, replacement, &[450]).is_err());
		// A target without the replacement, or a commit at another index.
		let mut absent = target.clone();
		absent.members = vec![other];
		let absent_digest = crate::consensus::decisions::generations::target_digest(&absent);
		let mut cert_absent = certificate.clone();
		cert_absent.target_digest = absent_digest;
		let events_absent = vec![
			GenerationEvent::Target {
				index: 600,
				target: absent,
				digest: absent_digest,
			},
			report(620, replacement, absent_digest),
			GenerationEvent::Commit { index: 700, gid: 2 },
		];
		assert!(certified_with(&events_absent, &cert_absent, replacement, &[450]).is_err());
		let mut moved = certificate.clone();
		moved.committed_at = 701;
		assert!(certified_with(&events, &moved, replacement, &[450]).is_err());
	}

	/// Codex 01a1e655 R2: a complete range passes; a purged prefix, an interior hole, a
	/// duplicate and a short range are INCOMPLETE EVIDENCE.
	#[test]
	fn a_scan_must_cover_its_range_exactly() {
		assert_eq!(contiguous(11..=15, 10, 15), Ok(()));
		assert_eq!(contiguous(std::iter::empty(), 10, 10), Ok(()));
		for (indices, why) in [
			(vec![13, 14, 15], "purged prefix"),
			(vec![11, 12, 14, 15], "interior hole"),
			(vec![11, 12, 12, 13, 14, 15], "duplicate"),
			(vec![11, 12, 13], "short"),
		] {
			let result = contiguous(indices, 10, 15);
			assert!(
				result
					.as_ref()
					.is_err_and(|error| error.starts_with("INCOMPLETE EVIDENCE")),
				"{why}: {result:?}"
			);
		}
	}

	/// Codex 01a1e655 R1: an earlier valid certificate that does not cover the designated
	/// reservation is not waited for; the later covering one is, once its exact commit is
	/// scanned; and its evidence takes the commit at the certificate's index, not an earlier
	/// same-gid attempt.
	#[test]
	fn the_pilot_waits_for_the_covering_certificate_and_its_exact_commit() {
		let replacement = Signer {
			node: 42,
			incarnation: StoreIncarnation::from_raw(9),
		};
		let other = Signer {
			node: 41,
			incarnation: StoreIncarnation::from_raw(1),
		};
		let target = |gid, t| Target {
			gid,
			t,
			predecessor_gid: gid - 1,
			members: vec![other, replacement],
			chunk_bits: 0,
			survivor_policy: 0,
		};
		let certificate = |target: &Target, committed_at| Certificate {
			gid: target.gid,
			target_digest: crate::consensus::decisions::generations::target_digest(target),
			t: target.t,
			predecessor_gid: target.predecessor_gid,
			chunks: Vec::new(),
			committed_at,
		};
		let designated = [450];
		// Generation 2 is valid but its cut (400) does not cover reservation 450.
		let early = target(2, 400);
		let early_cert = certificate(&early, 520);
		let mut events = vec![GenerationEvent::Commit { index: 520, gid: 2 }];
		assert!(!covering_certificate_ready(
			&events,
			Some(&early_cert),
			1,
			&designated
		));
		// Generation 3 covers it; an earlier attempt to commit it (index 690) is not its commit.
		let covering = target(3, 500);
		let digest = crate::consensus::decisions::generations::target_digest(&covering);
		let covering_cert = certificate(&covering, 700);
		events.extend([
			GenerationEvent::Target {
				index: 600,
				target: covering.clone(),
				digest,
			},
			GenerationEvent::Report {
				index: 620,
				gid: 3,
				digest,
				reporter: replacement,
				signer: replacement,
			},
			GenerationEvent::Commit { index: 690, gid: 3 },
		]);
		assert!(
			!covering_certificate_ready(&events, Some(&covering_cert), 1, &designated),
			"its exact commit is not scanned yet"
		);
		events.push(GenerationEvent::Commit { index: 700, gid: 3 });
		assert!(covering_certificate_ready(
			&events,
			Some(&covering_cert),
			1,
			&designated
		));
		assert_eq!(
			certified_with(&events, &covering_cert, replacement, &designated),
			Ok((600, 620, 700))
		);
		// Not past G, or nothing designated: never ready.
		assert!(!covering_certificate_ready(
			&events,
			Some(&covering_cert),
			3,
			&designated
		));
		assert!(!covering_certificate_ready(
			&events,
			Some(&covering_cert),
			1,
			&[]
		));
		assert!(!covering_certificate_ready(&events, None, 1, &designated));
	}

	/// Codex 01a1e637 R2: a result observed after its absolute deadline is refused even when
	/// it was ready at once; within the deadline it is accepted; work that is not done by the
	/// deadline is refused there.
	#[tokio::test]
	async fn a_result_observed_after_the_workflow_deadline_is_refused() {
		let expired = Instant::now() - Duration::from_millis(1);
		let late = within(expired, "a ready result", async { 7 }).await;
		assert!(late.is_err_and(|why| why.contains("after its deadline")));
		let open = Instant::now() + Duration::from_secs(5);
		assert_eq!(within(open, "a ready result", async { 7 }).await, Ok(7));
		let soon = Instant::now() + Duration::from_millis(20);
		let never = within(soon, "pending work", std::future::pending::<()>()).await;
		assert!(never.is_err_and(|why| why.contains("not done by its deadline")));
	}

	/// The witness accepts exactly a post-step, leader-stamped renewal of the same tenure that
	/// the applied lease equals, expiring past the margin; and refuses each lookalike.
	#[test]
	fn the_renewal_witness_refuses_old_future_hlc_queued_and_new_lineage_grants() {
		let before = lease(W - 1_000, W + 2_000);
		let pre = view(10, before, lineage(4), W - 1);
		let renewed = lease(W + 5, W + 3_005);
		let post = view(12, renewed, lineage(4), W + 5);
		let good = entry(11, W + 5, renewed, 4);
		assert_eq!(
			renewal_witness(&pre, &post, &[good.clone()], W, MARGIN, RANGE),
			Ok(11)
		);
		// A grant already applied before the step (index at or below the pre-step mark).
		let old = entry(10, W + 5, renewed, 4);
		assert!(renewal_witness(&pre, &post, &[old], W, MARGIN, RANGE).is_err());
		// A future HLC: the holder's lease start is past W, but the leader stamped it before.
		let future = entry(11, W - 1, renewed, 4);
		assert!(renewal_witness(&pre, &post, &[future], W, MARGIN, RANGE).is_err());
		// A queued pre-step grant applying after the step: its stamp is pre-step.
		let queued_lease = lease(W - 10, W + 2_990);
		let queued = entry(11, W - 10, queued_lease, 4);
		let post_queued = view(12, queued_lease, lineage(4), W + 5);
		assert!(renewal_witness(&pre, &post_queued, &[queued], W, MARGIN, RANGE).is_err());
		// A new lineage (a regrant or a takeover is no renewal).
		let post_new = view(12, renewed, lineage(5), W + 5);
		assert!(
			renewal_witness(
				&pre,
				&post_new,
				&[entry(11, W + 5, renewed, 5)],
				W,
				MARGIN,
				RANGE
			)
			.is_err()
		);
		// The applied lease is not the witnessed grant (it was denied).
		let other = lease(W + 6, W + 3_006);
		assert!(
			renewal_witness(
				&pre,
				&view(12, other, lineage(4), W + 5),
				&[good.clone()],
				W,
				MARGIN,
				RANGE
			)
			.is_err()
		);
		// A renewal that does not reach past the margin.
		let short = lease(W + 5, W + MARGIN);
		assert!(
			renewal_witness(
				&pre,
				&view(12, short, lineage(4), W + 5),
				&[entry(11, W + 5, short, 4)],
				W,
				MARGIN,
				RANGE
			)
			.is_err()
		);
		// A grant that does not continue the pre-step lease (begins after it lapsed).
		let gap = lease(W + 2_500, W + 5_500);
		assert!(
			renewal_witness(
				&pre,
				&view(12, gap, lineage(4), W + 5),
				&[entry(11, W + 5, gap, 4)],
				W,
				MARGIN,
				RANGE
			)
			.is_err()
		);
	}

	/// After a step, an owner-less voter's fresh pre-step sample (a delayed reading) keeps the
	/// step unrestored; once the real lease slack is spent the step fails with that state, and
	/// a restoration observed after its deadline is refused even when nothing is missing.
	#[test]
	fn a_stale_sample_without_slack_fails_and_a_late_restoration_is_refused() {
		let full: BTreeSet<u32> = [1, 2].into_iter().collect();
		let before = lease(W - 1_000, W + 2_000);
		let pre = view(10, before, lineage(4), W - 1);
		let renewed = lease(W + 5, W + 3_005);
		let post = view(12, renewed, lineage(4), W + 5);
		let entries = [entry(11, W + 5, renewed, 4)];
		let samples = [
			(2, W + 1, Duration::ZERO),
			(3, W - 700, Duration::from_millis(100)),
		];
		let missing = judge(
			&pre,
			&post,
			&entries,
			&samples,
			W + 5,
			W,
			MARGIN,
			&full,
			W - 1,
		);
		assert!(
			missing
				.iter()
				.any(|why| why.starts_with("voter 3: a fresh pre-step sample")),
			"{missing:?}"
		);
		let deadline = Instant::now();
		assert_eq!(
			conclude(&missing, deadline, deadline),
			None,
			"not yet failed within its slack"
		);
		let late = deadline + Duration::from_millis(1);
		let failed = conclude(&missing, late, deadline).expect("concluded");
		assert!(failed.is_err_and(|why| why.contains("voter 3")));
		// Restored, but observed late: refused.
		let restored = [(2, W + 1, Duration::ZERO)];
		let nothing = judge(
			&pre,
			&post,
			&entries,
			&restored,
			W + 5,
			W,
			MARGIN,
			&full,
			W - 1,
		);
		assert!(nothing.is_empty(), "{nothing:?}");
		assert_eq!(conclude(&nothing, deadline, deadline), Some(Ok(())));
		assert!(conclude(&nothing, late, deadline).is_some_and(|result| result.is_err()));
	}

	/// The test-directed sampling entry is the ordinary authenticated CUT: after a step the
	/// leader holds the follower's pre-step sample; the entry records a post-step one; nothing
	/// else was edited.
	#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
	async fn a_test_directed_sample_is_recorded_at_the_leader_through_the_ordinary_cut() {
		let time = TestTime::new();
		let mut cluster = {
			let time = time.clone();
			Cluster::start_with(move |_, cfg| {
				cfg.test_time = Some(time.clone());
				None
			})
			.await
		};
		let leader = cluster.leader().await;
		let follower = *NODES.iter().find(|id| **id != leader).unwrap();
		let proposals = cluster
			.node(follower)
			.data_server
			.decision_proposals_for_test();
		assert!(matches!(
			proposals.test_directed_sample().await,
			CutResponse::Cut(_)
		));
		let w = time.clock.advance(1_000);
		let sample = |cluster: &Cluster| {
			samples_at(cluster, leader)
				.into_iter()
				.find(|(peer, _, _)| *peer == follower)
				.map(|(_, remote, _)| remote)
		};
		assert!(
			sample(&cluster).is_some_and(|remote| remote < w),
			"a pre-step sample first"
		);
		assert!(matches!(
			proposals.test_directed_sample().await,
			CutResponse::Cut(_)
		));
		assert!(
			sample(&cluster).is_some_and(|remote| remote >= w),
			"a post-step sample"
		);
		for node in cluster.nodes.drain(..) {
			let id = node.local_node.id;
			tokio::time::timeout(Duration::from_secs(30), node.shutdown())
				.await
				.unwrap_or_else(|_| panic!("node {id} did not shut down within its bound"))
				.unwrap_or_else(|error| panic!("node {id} shutdown: {error}"));
		}
		assert!(
			time.abort.latched().is_empty(),
			"{:?}",
			time.abort.latched()
		);
	}

	/// The protocol in a real three-node ParentDecisions cluster: twenty seconds of wall time
	/// in restored steps, each with its gate samples, a post-step stamped commit and applied
	/// renewals of every Full lease, no takeover and nothing latched.
	#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
	async fn restored_steps_advance_a_three_node_cluster_without_a_takeover() {
		let time = TestTime::new();
		let (mut cluster, leader, descriptor) = ready_with({
			let time = time.clone();
			Cluster::start_with(move |_, cfg| {
				cfg.test_time = Some(time.clone());
				None
			})
		})
		.await;
		// A write seals the placement, so lineages and leases exist to renew.
		let meta = write_meta(&descriptor, 1, uuid_v7_at(time.clock.now_ms()));
		assert_committed(
			&write_through(&cluster, leader, meta, named_rows(&descriptor, &[(1, "a")])).await,
			"a write",
		);
		let full: BTreeSet<u32> = NODES.into_iter().collect();
		let start = time.clock.now_ms();
		let mut record = Record::default();
		let result = advance_to(
			&cluster,
			&time,
			&full,
			start + 20_000,
			Instant::now() + Duration::from_secs(120),
			&mut record,
		)
		.await;
		eprintln!("DST control record: {record:?}");
		result.unwrap();
		assert!(record.steps >= 14, "{record:?}");
		assert!(
			record.renewals >= record.steps,
			"a renewal witnessed every step: {record:?}"
		);
		assert!(time.clock.now_ms() >= start + 20_000);
		assert!(record.wall_advanced_ms > 0 && record.wall_advanced_ms <= 20_000);
		for node in cluster.nodes.drain(..) {
			let id = node.local_node.id;
			tokio::time::timeout(Duration::from_secs(30), node.shutdown())
				.await
				.unwrap_or_else(|_| panic!("node {id} did not shut down within its bound"))
				.unwrap_or_else(|error| panic!("node {id} shutdown: {error}"));
		}
		assert!(
			time.abort.latched().is_empty(),
			"{:?}",
			time.abort.latched()
		);
	}
}
