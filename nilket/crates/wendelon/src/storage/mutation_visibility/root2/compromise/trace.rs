//! The audit's per-entry record, taken inside the shared replay dispatcher after each Root2
//! carrier has been verified and reduced by the production path. It never verifies for the
//! reducer and never alters the fold: it reads the exact pre-entry and post-entry states, keeps
//! every role key it has seen with its activation coordinate (keys are never dropped after a
//! rotation), and records the reducer's result verbatim beside the audit's own signature findings.
use super::attribution::{Record2, records2};
use crate::storage::mutation_visibility::root2::admin_install::{
	InstalledAdmin2, install_admin2, original_roles3,
};
use crate::storage::mutation_visibility::root2::command::{ApplyResult2, Command2};
use crate::storage::mutation_visibility::root2::origin::{
	MutationAuthorityState2, root_and_origin2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::PinnedBoundTuple2;
use crate::storage::mutation_visibility::root2::role_epochs::RoleEpochs3;
use crate::storage::mutation_visibility::root2::types::{Digest2, Log2};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Per role (admin, governance, recovery), epoch `e` at index `e`: its key and activation.
#[derive(Clone, Default)]
pub(super) struct History2 {
	epochs: [Vec<(Digest2, Log2)>; 3],
}
impl History2 {
	/// Role codes are the proof codes: 1 admin, 2 governance, 3 recovery.
	pub(super) fn key(&self, role: u8, epoch: u64) -> Option<Digest2> {
		self.slot(role, epoch).map(|(key, _)| key)
	}
	pub(super) fn activated(&self, role: u8, epoch: u64) -> Option<Log2> {
		self.slot(role, epoch).map(|(_, at)| at)
	}
	/// The epoch active for `role` at this point of the replay.
	pub(super) fn active(&self, role: u8) -> Option<u64> {
		let history = self.epochs.get(usize::from(role.checked_sub(1)?))?;
		u64::try_from(history.len()).ok()?.checked_sub(1)
	}
	/// Every `(role, epoch)` whose key is `key`.
	pub(super) fn find(&self, key: &Digest2) -> Vec<(u8, u64)> {
		let mut out = Vec::new();
		for (role, history) in (1u8..).zip(&self.epochs) {
			for (epoch, (k, _)) in (0u64..).zip(history) {
				if k == key {
					out.push((role, epoch));
				}
			}
		}
		out
	}
	fn slot(&self, role: u8, epoch: u64) -> Option<(Digest2, Log2)> {
		let history = self.epochs.get(usize::from(role.checked_sub(1)?))?;
		history.get(usize::try_from(epoch).ok()?).copied()
	}
	#[cfg(test)]
	pub(super) fn from_epochs(epochs: [Vec<(Digest2, Log2)>; 3]) -> Self {
		Self { epochs }
	}
}

/// The reducer's verbatim result: applied (with its result kind) or a consumed denial.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Effect2 {
	Applied(&'static str),
	Denied(u16),
}
pub(super) struct Step2 {
	pub(super) at: Log2,
	pub(super) fingerprint: Digest2,
	pub(super) kind: &'static str,
	pub(super) digest: Digest2,
	pub(super) effect: Effect2,
	pub(super) records: Vec<Record2>,
}

/// Accumulated by the shared dispatcher; read by the report once the fold has closed and bound.
pub(in crate::storage::mutation_visibility::root2) struct Trace2 {
	admin: Option<InstalledAdmin2>,
	history: History2,
	steps: Vec<Step2>,
	freezes: Vec<(Log2, Option<Log2>)>,
	retired: Option<Log2>,
	fault: Option<&'static str>,
	// One past the index of the last entry the dispatcher took up (0: none). Shared with the
	// caller, so the entry a refusal stopped at survives the discarded accumulator.
	attempted: Arc<AtomicU64>,
}
/// What the fold established, for the report.
pub(super) struct Traced2 {
	pub(super) admin: InstalledAdmin2,
	pub(super) history: History2,
	pub(super) steps: Vec<Step2>,
	pub(super) freezes: Vec<(Log2, Option<Log2>)>,
	pub(super) retired: Option<Log2>,
}
fn epochs_of(state: &MutationAuthorityState2) -> Option<&RoleEpochs3> {
	root_and_origin2(state).and_then(|(root, _)| root.state.role_epochs.as_deref())
}
impl Trace2 {
	pub(super) fn new() -> Self {
		Self {
			admin: None,
			history: History2::default(),
			steps: Vec::new(),
			freezes: Vec::new(),
			retired: None,
			fault: None,
			attempted: Arc::new(AtomicU64::new(0)),
		}
	}
	/// The caller's handle on the last attempted index.
	pub(super) fn attempted(&self) -> Arc<AtomicU64> {
		Arc::clone(&self.attempted)
	}
	/// The dispatcher is taking up the entry at `index`. Frozen once the trace has faulted, so
	/// the diagnostic names the entry that faulted, not the last one before the stop.
	pub(in crate::storage::mutation_visibility::root2) fn attempt(&mut self, index: u64) {
		if self.fault.is_none() {
			self.attempted
				.store(index.saturating_add(1), Ordering::Relaxed);
		}
	}
	/// One verified, reduced Root2 carrier entry at `at`. The first is the install entry: the
	/// administrator view (the root key) and the epoch-0 keys come from its post-entry state.
	#[allow(clippy::too_many_arguments)]
	pub(in crate::storage::mutation_visibility::root2) fn entry(
		&mut self,
		pin: &PinnedBoundTuple2,
		pre: &MutationAuthorityState2,
		at: Log2,
		fingerprint: Digest2,
		command: &Command2,
		governance: Option<&[u8]>,
		result: &ApplyResult2,
		post: &MutationAuthorityState2,
	) {
		if self.fault.is_some() {
			return;
		}
		let Some((root, _)) = root_and_origin2(post) else {
			self.fault = Some("post-entry state is not rooted");
			return;
		};
		let digest = match command.digest(root.state.generation, root.state.cluster) {
			Ok(digest) => digest,
			Err(_) => {
				self.fault = Some("command digest");
				return;
			}
		};
		if self.admin.is_none() {
			if !matches!(command, Command2::Bootstrap { .. }) {
				self.fault = Some("first Root2 entry is not the install entry");
				return;
			}
			let installed = install_admin2(post, pin).ok();
			let Some(original) = installed.as_ref().and_then(original_roles3) else {
				self.fault = Some("install entry did not yield a profile-3 administrator");
				return;
			};
			for (role, key) in original.into_iter().enumerate() {
				self.history.epochs[role].push((key, at));
			}
			self.admin = installed;
		}
		let records = records2(
			command,
			governance,
			self.admin.as_ref(),
			&self.history,
			digest,
		);
		if let Err(fault) = self.advance(pre, post, at) {
			self.fault = Some(fault);
			return;
		}
		self.steps.push(Step2 {
			at,
			fingerprint,
			kind: kind_name(command),
			digest,
			effect: effect_of(result),
			records,
		});
	}
	/// Successor activations, freeze transitions and retirement, from the two states.
	fn advance(
		&mut self,
		pre: &MutationAuthorityState2,
		post: &MutationAuthorityState2,
		at: Log2,
	) -> Result<(), &'static str> {
		let (before, after) = (epochs_of(pre), epochs_of(post));
		for role in 0..3 {
			let had = before.map_or(0, |e| e.successors[role].len());
			if self.history.epochs[role].len() != had + 1 {
				return Err("replayed key history diverged from the pre-entry state");
			}
			for successor in after
				.map_or(&[][..], |e| &e.successors[role][..])
				.iter()
				.skip(had)
			{
				self.history.epochs[role].push((successor.key, successor.activated));
			}
		}
		let (was, is) = (
			before.is_some_and(|e| e.frozen),
			after.is_some_and(|e| e.frozen),
		);
		match (was, is) {
			(false, true) => self.freezes.push((at, None)),
			(true, false) => match self.freezes.last_mut() {
				Some((_, end @ None)) => *end = Some(at),
				_ => return Err("unfreeze without an open freeze"),
			},
			_ => {}
		}
		if self.retired.is_none() {
			self.retired = after.and_then(|e| e.retired.as_ref()).map(|r| r.at);
		}
		Ok(())
	}
	pub(super) fn finish(self) -> Result<Traced2, &'static str> {
		if let Some(fault) = self.fault {
			return Err(fault);
		}
		Ok(Traced2 {
			admin: self.admin.ok_or("no install entry was replayed")?,
			history: self.history,
			steps: self.steps,
			freezes: self.freezes,
			retired: self.retired,
		})
	}
}
fn effect_of(result: &ApplyResult2) -> Effect2 {
	match result {
		ApplyResult2::Denied { reason, .. } => Effect2::Denied(*reason),
		ApplyResult2::Covered => Effect2::Applied("covered"),
		ApplyResult2::AlreadyProcessed => Effect2::Applied("already_processed"),
		ApplyResult2::Registered(_) => Effect2::Applied("registered"),
		ApplyResult2::Granted(_) => Effect2::Applied("granted"),
		ApplyResult2::Closed { .. } => Effect2::Applied("closed"),
		ApplyResult2::Parent { .. } => Effect2::Applied("parent"),
		ApplyResult2::PendingEvidence(_) => Effect2::Applied("pending_evidence"),
		ApplyResult2::RejectedEvidence(_) => Effect2::Applied("rejected_evidence"),
		ApplyResult2::Permission { .. } => Effect2::Applied("permission"),
	}
}
fn kind_name(command: &Command2) -> &'static str {
	match command {
		Command2::Bootstrap { .. } => "bootstrap",
		Command2::Cover { .. } => "cover",
		Command2::EnrollSelector(_) => "enroll_selector",
		Command2::EnrollAuthority { .. } => "enroll_authority",
		Command2::RevokeAuthority(_) => "revoke_authority",
		Command2::EnrollLeaseIssuer { .. } => "enroll_lease_issuer",
		Command2::RevokeLeaseIssuer { .. } => "revoke_lease_issuer",
		Command2::Grant { .. } => "grant",
		Command2::Release { .. } => "release",
		Command2::Cut { .. } => "cut",
		Command2::Reserve(_) => "reserve",
		Command2::Finalize { .. } => "finalize",
		Command2::Resolve { .. } => "resolve",
		Command2::AdmissionPermission(_) => "admission_permission",
		Command2::RotateAdmin(_) => "rotate_admin",
		Command2::RotateGovernance(_) => "rotate_governance",
		Command2::RotateRecovery(_) => "rotate_recovery",
		Command2::EpochAdmissionPermission(_) => "epoch_admission_permission",
		Command2::Freeze(_) => "freeze",
		Command2::Unfreeze(_) => "unfreeze",
		Command2::RetireRoot => "retire_root",
	}
}

#[cfg(test)]
mod tests {
	use super::Trace2;
	use std::sync::atomic::Ordering;

	/// The trace-fault branch is not reachable from a real history (a bound profile-3 root's
	/// reducer never yields those states), so the latch is pinned here directly.
	#[test]
	fn attempted_index_latches_at_the_first_trace_fault() {
		let mut trace = Trace2::new();
		let attempted = trace.attempted();
		trace.attempt(4);
		assert_eq!(attempted.load(Ordering::Relaxed), 5);
		trace.fault = Some("synthetic");
		trace.attempt(9);
		assert_eq!(
			attempted.load(Ordering::Relaxed),
			5,
			"the faulted entry stays named"
		);
	}
}
