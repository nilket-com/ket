//! Dormant replay progress and recovery mark (Q3: authority is rebuilt only by contiguous replay
//! from the committed genesis). The monitor decides whether states the caller says it replayed
//! started at genesis, matched the saved high-water mark and reached the expected target tip. It
//! authenticates no entry, cannot tell whether the caller substituted coherent states (that rests
//! on the held committed-entry producers and replay loop), never builds, decodes or promotes state,
//! and performs no I/O. `Ready` is progress-readiness only: serving additionally requires binding
//! to the expected deployment (`install_admin2` with the pinned tuple), then durable persistence
//! of the new mark, then serving, all held. A `Provisioned` baseline is owner-provisioned data,
//! not proof that a reset was authorized. Full state encoding happens at most twice per replay:
//! at the saved-mark comparison and when producing the final mark.
use crate::storage::mutation_visibility::redacted_debug;
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
use crate::storage::mutation_visibility::root2::origin::{
	MutationAuthorityState2, coverage2, encode_authority2, root_and_origin2,
};
use crate::storage::mutation_visibility::root2::types::{Digest2, Log2};

const MAGIC: &[u8; 8] = b"VISRCM02";
const VERSION: u16 = 1;
const STATE_DOMAIN: &[u8] = b"VISROOT2-RECOVERY-STATE\0";
pub(in crate::storage::mutation_visibility::root2) const RECOVERY_MARK_BYTES2: usize = 159;
const GENESIS: EntryPosition2 = EntryPosition2 {
	term: 0,
	node: 0,
	index: 0,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct RecoveryMark2 {
	pub generation: [u8; 16],
	pub cluster: [u8; 16],
	pub cut: Log2,
	pub fingerprint: Digest2,
	pub command: Digest2,
	pub state: Digest2,
}
redacted_debug!(RecoveryMark2, "RecoveryMark2");
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum Baseline2 {
	Provisioned,
	Mark(RecoveryMark2),
}
redacted_debug!(Baseline2, "Baseline2");
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct ReplayTarget2 {
	pub cut: Log2,
	pub fingerprint: Digest2,
	pub command: Digest2,
}
redacted_debug!(ReplayTarget2, "ReplayTarget2");
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) struct Deployment2 {
	pub generation: [u8; 16],
	pub cluster: [u8; 16],
}
redacted_debug!(Deployment2, "Deployment2");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum FieldError2 {
	Generation,
	Cluster,
	Cut,
	Target,
	Deployment,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum RecoveryError2 {
	Length,
	Magic,
	Version,
	Tag,
	Padding,
	Field(FieldError2),
	NotRooted,
	Unavailable,
	NotGenesis,
	Regressed,
	Conflict,
	TargetBelowMark,
	Overshoot,
	Latched,
	Codec,
}
#[derive(Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum Progress2 {
	Replaying,
	MarkMatched,
	Ready(RecoveryMark2),
}
redacted_debug!(Progress2, "Progress2");
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase2 {
	Open,
	Finished,
	Failed,
}
/// One replay attempt; a restart is a new monitor. Errors and `Ready` latch it.
pub(in crate::storage::mutation_visibility::root2) struct ReplayMonitor2 {
	deployment: Deployment2,
	baseline: Baseline2,
	target: ReplayTarget2,
	phase: Phase2,
	last: Option<EntryPosition2>,
	rooted: bool,
	matched: bool,
	#[cfg(test)]
	encodings: u32,
}
redacted_debug!(ReplayMonitor2, "ReplayMonitor2");
type Result<T> = std::result::Result<T, RecoveryError2>;

fn valid_log(l: Log2) -> bool {
	l.term != 0 && l.node != 0 && l.index != 0
}
// Shared by encode, decode and monitor construction. Digests are arbitrary 32-byte values.
fn check_mark(m: &RecoveryMark2) -> Result<()> {
	use FieldError2 as F;
	if m.generation == [0; 16] {
		return Err(RecoveryError2::Field(F::Generation));
	}
	if m.cluster == [0; 16] {
		return Err(RecoveryError2::Field(F::Cluster));
	}
	if !valid_log(m.cut) {
		return Err(RecoveryError2::Field(F::Cut));
	}
	Ok(())
}
fn state_digest(state: &MutationAuthorityState2) -> Result<Digest2> {
	let bytes = encode_authority2(state).map_err(|_| RecoveryError2::Codec)?;
	Ok(hash(STATE_DOMAIN, &bytes))
}

/// Canonical 159 bytes: magic, version, tag (1 Provisioned with zero padding, 2 Mark), generation,
/// cluster, cut (term, node, index), fingerprint, command, state.
pub(in crate::storage::mutation_visibility::root2) fn encode_recovery_mark2(
	b: &Baseline2,
) -> Result<[u8; RECOVERY_MARK_BYTES2]> {
	let mut out = [0u8; RECOVERY_MARK_BYTES2];
	out[..8].copy_from_slice(MAGIC);
	out[8..10].copy_from_slice(&VERSION.to_le_bytes());
	match b {
		Baseline2::Provisioned => out[10] = 1,
		Baseline2::Mark(m) => {
			check_mark(m)?;
			out[10] = 2;
			out[11..27].copy_from_slice(&m.generation);
			out[27..43].copy_from_slice(&m.cluster);
			out[43..51].copy_from_slice(&m.cut.term.to_le_bytes());
			out[51..55].copy_from_slice(&m.cut.node.to_le_bytes());
			out[55..63].copy_from_slice(&m.cut.index.to_le_bytes());
			out[63..95].copy_from_slice(&m.fingerprint);
			out[95..127].copy_from_slice(&m.command);
			out[127..159].copy_from_slice(&m.state);
		}
	}
	Ok(out)
}
pub(in crate::storage::mutation_visibility::root2) fn decode_recovery_mark2(
	b: &[u8],
) -> Result<Baseline2> {
	use RecoveryError2 as E;
	if b.len() != RECOVERY_MARK_BYTES2 {
		return Err(E::Length);
	}
	if &b[..8] != MAGIC {
		return Err(E::Magic);
	}
	if u16::from_le_bytes([b[8], b[9]]) != VERSION {
		return Err(E::Version);
	}
	let array = |at: usize| -> [u8; 32] { b[at..at + 32].try_into().expect("fixed mark offset") };
	match b[10] {
		1 if b[11..].iter().all(|x| *x == 0) => Ok(Baseline2::Provisioned),
		1 => Err(E::Padding),
		2 => {
			let m = RecoveryMark2 {
				generation: b[11..27].try_into().expect("fixed mark offset"),
				cluster: b[27..43].try_into().expect("fixed mark offset"),
				cut: Log2 {
					term: u64::from_le_bytes(b[43..51].try_into().expect("fixed mark offset")),
					node: u32::from_le_bytes(b[51..55].try_into().expect("fixed mark offset")),
					index: u64::from_le_bytes(b[55..63].try_into().expect("fixed mark offset")),
				},
				fingerprint: array(63),
				command: array(95),
				state: array(127),
			};
			check_mark(&m)?;
			Ok(Baseline2::Mark(m))
		}
		_ => Err(E::Tag),
	}
}
/// The recovery mark of a rooted state: its covered tip and the digest of its full encoding.
pub(in crate::storage::mutation_visibility::root2) fn mark_of2(
	state: &MutationAuthorityState2,
) -> Result<RecoveryMark2> {
	let Some((root, _)) = root_and_origin2(state) else {
		return Err(RecoveryError2::NotRooted);
	};
	let s = &root.state;
	Ok(RecoveryMark2 {
		generation: s.generation,
		cluster: s.cluster,
		cut: s.covered,
		fingerprint: s.fingerprint,
		command: s.command_digest,
		state: state_digest(state)?,
	})
}
/// Construction checks, before any replay: deployment, target, the baseline's presence and
/// validity, its identity, and the target against the mark.
pub(in crate::storage::mutation_visibility::root2) fn replay_monitor2(
	deployment: Deployment2,
	baseline: Option<Baseline2>,
	target: ReplayTarget2,
) -> Result<ReplayMonitor2> {
	use RecoveryError2 as E;
	if deployment.generation == [0; 16] || deployment.cluster == [0; 16] {
		return Err(E::Field(FieldError2::Deployment));
	}
	if !valid_log(target.cut) {
		return Err(E::Field(FieldError2::Target));
	}
	let baseline = baseline.ok_or(E::Unavailable)?;
	if let Baseline2::Mark(m) = baseline {
		check_mark(&m)?;
		if (m.generation, m.cluster) != (deployment.generation, deployment.cluster) {
			return Err(E::Conflict);
		}
		if target.cut.index < m.cut.index {
			return Err(E::TargetBelowMark);
		}
		if target.cut.index == m.cut.index
			&& (target.cut, target.fingerprint, target.command) != (m.cut, m.fingerprint, m.command)
		{
			return Err(E::Conflict);
		}
	}
	Ok(ReplayMonitor2 {
		deployment,
		baseline,
		target,
		phase: Phase2::Open,
		last: None,
		rooted: false,
		matched: matches!(baseline, Baseline2::Provisioned),
		#[cfg(test)]
		encodings: 0,
	})
}
/// Observe the state after one replayed entry. Any error fails the attempt; `Ready` finishes it;
/// either latches the monitor. The durable mark and prior authority are never touched here.
pub(in crate::storage::mutation_visibility::root2) fn observe2(
	m: &mut ReplayMonitor2,
	state: &MutationAuthorityState2,
) -> Result<Progress2> {
	if m.phase != Phase2::Open {
		return Err(RecoveryError2::Latched);
	}
	let r = observe(m, state);
	match r {
		Err(_) => m.phase = Phase2::Failed,
		Ok(Progress2::Ready(_)) => m.phase = Phase2::Finished,
		Ok(_) => {}
	}
	r
}
fn observe(m: &mut ReplayMonitor2, state: &MutationAuthorityState2) -> Result<Progress2> {
	use RecoveryError2 as E;
	// 1. Empty: an inspection before genesis never advances; after any observation it is an error.
	let Some(pos) = coverage2(state) else {
		return if m.last.is_none() {
			Ok(Progress2::Replaying)
		} else {
			Err(E::Regressed)
		};
	};
	let rooted = root_and_origin2(state);
	// 2. Genesis first, then strict +1 contiguity; a pre-root may not follow the root.
	match m.last {
		None => {
			if rooted.is_some() || pos != GENESIS {
				return Err(E::NotGenesis);
			}
		}
		Some(last) => {
			let next = last.index.checked_add(1).ok_or(E::Regressed)?;
			if pos.index != next || (m.rooted && rooted.is_none()) {
				return Err(E::Regressed);
			}
		}
	}
	// 3-4. Record progress; nothing may pass the target, rooted or not.
	m.last = Some(pos);
	if pos.index > m.target.cut.index {
		return Err(E::Overshoot);
	}
	// 5. Pre-root.
	let Some((root, _)) = rooted else {
		return Ok(Progress2::Replaying);
	};
	m.rooted = true;
	let s = &root.state;
	// 6. The deployment the replay must describe.
	if (s.generation, s.cluster) != (m.deployment.generation, m.deployment.cluster) {
		return Err(E::Conflict);
	}
	// 7-8. The whole saved mark, at its cut; nothing above it without a match.
	let mut at_mark = false;
	if let Baseline2::Mark(mk) = m.baseline {
		if pos.index == mk.cut.index {
			if (s.covered, s.fingerprint, s.command_digest) != (mk.cut, mk.fingerprint, mk.command)
			{
				return Err(E::Conflict);
			}
			#[cfg(test)]
			{
				m.encodings += 1;
			}
			if state_digest(state)? != mk.state {
				return Err(E::Conflict);
			}
			m.matched = true;
			at_mark = true;
		} else if pos.index > mk.cut.index && !m.matched {
			return Err(E::Conflict);
		}
	}
	// 9. The expected target tip, only after the mark.
	if pos.index == m.target.cut.index {
		let t = m.target;
		if (s.covered, s.fingerprint, s.command_digest) != (t.cut, t.fingerprint, t.command)
			|| !m.matched
		{
			return Err(E::Conflict);
		}
		#[cfg(test)]
		{
			m.encodings += 1;
		}
		return Ok(Progress2::Ready(mark_of2(state)?));
	}
	// 10-11.
	Ok(if at_mark {
		Progress2::MarkMatched
	} else {
		Progress2::Replaying
	})
}
/// Test-only: full encodings performed by this monitor so far.
#[cfg(test)]
pub(super) fn encodings_for_test(m: &ReplayMonitor2) -> u32 {
	m.encodings
}
/// Test-only synthetic setup for contiguity overflow: places the monitor's last observed position
/// at `index`, which no feasible real history can reach. It builds no state or token.
#[cfg(test)]
pub(super) fn place_last_for_test(m: &mut ReplayMonitor2, index: u64, rooted: bool) {
	m.last = Some(EntryPosition2 {
		term: 1,
		node: 1,
		index,
	});
	m.rooted = rooted;
}
