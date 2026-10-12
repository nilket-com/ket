//! Dormant owned replay composition. Output is withheld until closure, administrator binding
//! and durable recovery-mark confirmation succeed. No runtime caller or serving authority.
//! Each batch is validated before its fold. Early folds stay provisional until the last batch;
//! the first boundary fingerprint is compared at the target, with no intermediate-read pinning.
use crate::consensus::WendelonTypeConfig;
use crate::consensus::retention::{ControlRetention, HistoryCheck};
use crate::storage::mutation_visibility::root2::acquisition::{
	AcquisitionCaps2, AcquisitionError2, AcquisitionSources2, FoldError2, PrefixValidated2,
	acquire_committed_prefix2,
};
use crate::storage::mutation_visibility::root2::admin_install::{InstallError2, install_admin2};
use crate::storage::mutation_visibility::root2::command::{ApplyResult2, Command2};
use crate::storage::mutation_visibility::root2::committed::{
	AdapterError2, CommittedSource2, committed_entry2,
};
use crate::storage::mutation_visibility::root2::durable::mark_store::{
	MarkIoError2, MarkStore2, MarkWrite2,
};
use crate::storage::mutation_visibility::root2::origin::{
	CoverError2, EntryClass2, MutationAuthorityState2, OriginView2, PreRootStep2, advance_cover2,
	advance_pre_root2, empty_authority2, origin_view2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::PinnedBoundTuple2;
use crate::storage::mutation_visibility::root2::recovery::{
	Baseline2, Deployment2, Progress2, RecoveryError2, RecoveryMark2, ReplayMonitor2,
	ReplayTarget2, TargetCommand2, mark_of2, observe2, replay_monitor2,
};
use crate::storage::mutation_visibility::root2::trust::{
	BindError2, CommittedEntry2, TrustedEntry2,
};
use crate::storage::mutation_visibility::root2::types::{Log2, RootError2};
use crate::storage::mutation_visibility::root2::verification::{
	UnsupportedKind2, VerifyError2, advance_verified2,
};
use openraft::Entry;
use openraft::storage::RaftLogReader;
use std::cell::RefCell;

/// Lexical run scope, constructible only here. It grants no permission or currentness.
pub(in crate::storage::mutation_visibility::root2) struct PrefixRun2 {
	_scope: (),
}
struct ReplayDriver2<'a> {
	deployment: Deployment2,
	baseline: Baseline2,
	pin: &'a PinnedBoundTuple2,
}
struct Provisional2 {
	state: MutationAuthorityState2,
	monitor: ReplayMonitor2,
	traversed: u64,
	progress: Progress2,
}
#[derive(Debug)]
pub(in crate::storage::mutation_visibility::root2) enum Fault2 {
	MonitorInit(RecoveryError2),
	TargetAdapt(AdapterError2),
	Adapt(AdapterError2),
	Bind(BindError2),
	PreRoot(RootError2),
	Cover(CoverError2),
	Verify(VerifyError2),
	Unsupported(UnsupportedKind2),
	NoCarrier,
	Observe(RecoveryError2),
	Internal,
}
#[derive(Debug)]
pub(in crate::storage::mutation_visibility::root2) enum ReplayError2 {
	Acquisition(AcquisitionError2),
	Fault(Fault2),
	Baseline(BaselineError2),
	Closure(ClosureError2),
	Binding(InstallError2),
	MarkEncode(RecoveryError2),
	Mark(MarkIoError2),
	Refused(MarkRefusal2),
}
#[derive(Debug, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum BaselineError2 {
	Unavailable,
}
#[derive(Debug, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum ClosureError2 {
	SourceClosed,
	Regressed,
	Conflict,
	Retention,
}
#[derive(Debug, PartialEq, Eq)]
pub(in crate::storage::mutation_visibility::root2) enum MarkRefusal2 {
	Identity,
	Regressed,
	Conflict,
	NotConfirmed,
}
/// Confirmed replay data at a local cut; it grants neither quorum currentness nor serving.
pub(in crate::storage::mutation_visibility::root2) struct Replayed2 {
	state: MutationAuthorityState2,
	mark: RecoveryMark2,
	report: PrefixValidated2,
}
impl Replayed2 {
	pub(in crate::storage::mutation_visibility::root2) fn state(&self) -> &MutationAuthorityState2 {
		&self.state
	}
	pub(in crate::storage::mutation_visibility::root2) fn mark(&self) -> RecoveryMark2 {
		self.mark
	}
	pub(in crate::storage::mutation_visibility::root2) fn report(&self) -> &PrefixValidated2 {
		&self.report
	}
}
/// Captures the confirmed baseline before acquisition. Until the synchronous mark write, all
/// state is provisional. Dropping the future cannot expose it; a successful mark write remains
/// durable even if the returned value is subsequently dropped.
pub(in crate::storage::mutation_visibility::root2) async fn run<
	Rd: RaftLogReader<WendelonTypeConfig>,
>(
	reader: &mut Rd,
	sources: &impl AcquisitionSources2,
	caps: AcquisitionCaps2,
	deployment: Deployment2,
	pin: &PinnedBoundTuple2,
	mark_store: &mut MarkStore2,
) -> Result<Replayed2, ReplayError2> {
	let baseline = mark_store
		.durable()
		.ok_or(ReplayError2::Baseline(BaselineError2::Unavailable))?;
	let (acc, report) = ReplayDriver2 {
		deployment,
		baseline,
		pin,
	}
	.compose(reader, sources, caps)
	.await?;
	let a = sources
		.applied()
		.map_err(|_| ReplayError2::Closure(ClosureError2::SourceClosed))?
		.ok_or(ReplayError2::Closure(ClosureError2::Regressed))?;
	let terminal = report.terminal_applied();
	if a.index < terminal.index {
		return Err(ReplayError2::Closure(ClosureError2::Regressed));
	}
	if a.index == terminal.index && a != terminal {
		return Err(ReplayError2::Closure(ClosureError2::Conflict));
	}
	let retention = sources.retention();
	if retention.configured != ControlRetention::Full
		|| !matches!(retention.history, HistoryCheck::Complete { .. })
		|| retention.violation.is_some()
	{
		return Err(ReplayError2::Closure(ClosureError2::Retention));
	}
	install_admin2(&acc.state, pin).map_err(ReplayError2::Binding)?;
	let mark = mark_of2(&acc.state).map_err(ReplayError2::MarkEncode)?;
	let outcome = mark_store
		.advance(&deployment, &mark)
		.map_err(map_mark_error)?;
	confirm_mark(outcome, mark_store.durable(), mark)?;
	Ok(Replayed2 {
		state: acc.state,
		mark,
		report,
	})
}
fn map_mark_error(e: MarkIoError2) -> ReplayError2 {
	match e {
		MarkIoError2::Identity => ReplayError2::Refused(MarkRefusal2::Identity),
		MarkIoError2::Regressed => ReplayError2::Refused(MarkRefusal2::Regressed),
		MarkIoError2::Conflict => ReplayError2::Refused(MarkRefusal2::Conflict),
		other => ReplayError2::Mark(other),
	}
}
fn confirm_mark(
	outcome: MarkWrite2,
	durable: Option<Baseline2>,
	mark: RecoveryMark2,
) -> Result<(), ReplayError2> {
	let expected = Baseline2::Mark(mark);
	if matches!(outcome, MarkWrite2::Unchanged(b) if b != expected) || durable != Some(expected) {
		return Err(ReplayError2::Refused(MarkRefusal2::NotConfirmed));
	}
	Ok(())
}
impl ReplayDriver2<'_> {
	/// Private intermediate only; run validates closure/binding and confirms the mark before
	/// exporting anything. Neither this method nor its output is accessible to siblings.
	async fn compose<Rd: RaftLogReader<WendelonTypeConfig>>(
		&self,
		reader: &mut Rd,
		sources: &impl AcquisitionSources2,
		caps: AcquisitionCaps2,
	) -> Result<(Provisional2, PrefixValidated2), ReplayError2> {
		let scope = PrefixRun2 { _scope: () };
		let source = CommittedSource2::acquired(&scope);
		let slot = RefCell::new(None);
		let result = acquire_committed_prefix2(
			reader,
			sources,
			caps,
			|boundary| {
				self.initialize(&source, boundary)
					.map_err(|e| store_fault(&slot, e))
			},
			|mut acc: Provisional2, batch| {
				for entry in batch {
					self.dispatch(&mut acc, &source, entry)
						.map_err(|e| store_fault(&slot, e))?;
				}
				Ok(acc)
			},
		)
		.await;
		match result {
			Err(AcquisitionError2::Fold(_)) => Err(ReplayError2::Fault(
				slot.into_inner().unwrap_or(Fault2::Internal),
			)),
			Err(e) => Err(ReplayError2::Acquisition(e)),
			Ok((acc, report)) => {
				if acc.traversed != report.entries() || !matches!(acc.progress, Progress2::Ready(_))
				{
					return Err(ReplayError2::Fault(Fault2::Internal));
				}
				Ok((acc, report))
			}
		}
	}
	fn initialize(
		&self,
		source: &CommittedSource2,
		boundary: &Entry<WendelonTypeConfig>,
	) -> Result<Provisional2, Fault2> {
		let committed = committed_entry2(source, boundary).map_err(Fault2::TargetAdapt)?;
		let (p, fingerprint, _) = committed.entry_fields();
		let target = ReplayTarget2 {
			cut: Log2 {
				term: p.term,
				node: p.node,
				index: p.index,
			},
			fingerprint,
			command: TargetCommand2::BoundAtCut,
		};
		let monitor = replay_monitor2(self.deployment, Some(self.baseline), target)
			.map_err(Fault2::MonitorInit)?;
		Ok(Provisional2 {
			state: empty_authority2(),
			monitor,
			traversed: 0,
			progress: Progress2::Replaying,
		})
	}
	fn dispatch(
		&self,
		acc: &mut Provisional2,
		source: &CommittedSource2,
		entry: &Entry<WendelonTypeConfig>,
	) -> Result<(), Fault2> {
		let committed = committed_entry2(source, entry).map_err(Fault2::Adapt)?;
		let (_, _, class) = committed.entry_fields();
		let state = match (origin_view2(&acc.state), class) {
			(OriginView2::Empty, _) => pre_root(&acc.state, &committed)?,
			(OriginView2::PreRoot { .. }, EntryClass2::Bootstrap(_)) => {
				self.command(&acc.state, &committed)?
			}
			(OriginView2::PreRoot { .. }, _) => pre_root(&acc.state, &committed)?,
			(OriginView2::Root, EntryClass2::Bootstrap(_) | EntryClass2::OtherRoot2(_)) => {
				self.command(&acc.state, &committed)?
			}
			(OriginView2::Root, _) => {
				let (next, result) =
					advance_cover2(&acc.state, &committed).map_err(Fault2::Cover)?;
				if matches!(result, ApplyResult2::AlreadyProcessed) {
					return Err(Fault2::Internal);
				}
				next
			}
		};
		// The sole observation in the dispatcher, on the state after this exact traversed entry.
		let progress = observe2(&mut acc.monitor, &state).map_err(Fault2::Observe)?;
		acc.state = state;
		acc.progress = progress;
		acc.traversed = acc.traversed.checked_add(1).ok_or(Fault2::Internal)?;
		Ok(())
	}
	fn command(
		&self,
		state: &MutationAuthorityState2,
		committed: &CommittedEntry2,
	) -> Result<MutationAuthorityState2, Fault2> {
		let command = command_at(committed)?;
		let trusted = TrustedEntry2::bind(
			committed,
			&command,
			self.deployment.generation,
			self.deployment.cluster,
		)
		.map_err(Fault2::Bind)?;
		let (next, result) = advance_verified2(state, self.pin, committed, &trusted, &command)
			.map_err(|e| match e {
				VerifyError2::Unsupported(k) => Fault2::Unsupported(k),
				other => Fault2::Verify(other),
			})?;
		if matches!(result, ApplyResult2::AlreadyProcessed) {
			return Err(Fault2::Internal);
		}
		Ok(next)
	}
}
fn pre_root(
	state: &MutationAuthorityState2,
	committed: &CommittedEntry2,
) -> Result<MutationAuthorityState2, Fault2> {
	let (next, step) = advance_pre_root2(state, committed).map_err(Fault2::PreRoot)?;
	if step == PreRootStep2::AlreadyProcessed {
		return Err(Fault2::Internal);
	}
	Ok(next)
}
fn store_fault(slot: &RefCell<Option<Fault2>>, e: Fault2) -> FoldError2 {
	*slot.borrow_mut() = Some(e);
	FoldError2
}
fn command_at(committed: &CommittedEntry2) -> Result<Command2, Fault2> {
	#[cfg(test)]
	{
		let (p, fp, _) = committed.entry_fields();
		return crate::storage::mutation_visibility::root2::committed::fixture::command(p, fp)
			.ok_or(Fault2::NoCarrier);
	}
	#[cfg(not(test))]
	{
		let _ = committed;
		Err(Fault2::NoCarrier)
	}
}
#[cfg(test)]
mod tests;
