//! L-S2 completion, durable refusal and cancellation pins. No production mock store.
use super::*;
use crate::storage::mutation_visibility::root2::acquisition::SourceClosed2;
use crate::storage::mutation_visibility::root2::durable::mark_store::{MarkStep2, seam};
use crate::test_support::TestDir;
use std::cell::Cell;
use std::os::unix::fs::PermissionsExt;

pub(super) fn marks() -> (TestDir, MarkStore2) {
	let d = TestDir::new("wendelon-replay-completion");
	std::fs::set_permissions(d.as_ref(), std::fs::Permissions::from_mode(0o700)).unwrap();
	let (mut s, _) = MarkStore2::open(d.as_ref()).unwrap();
	s.provision().unwrap();
	(d, s)
}
fn bytes(d: &TestDir) -> Vec<u8> {
	std::fs::read(d.as_ref().join("RECOVERY_MARK")).unwrap()
}
struct Closing {
	cut: LogId<u32>,
	calls: Cell<usize>,
	last: Result<Option<LogId<u32>>, SourceClosed2>,
	retention_calls: Cell<usize>,
	status: RetentionStatus,
}
impl Closing {
	fn new(cut: LogId<u32>) -> Self {
		Self {
			cut,
			calls: Cell::new(0),
			last: Ok(Some(cut)),
			retention_calls: Cell::new(0),
			status: full(),
		}
	}
}
impl AcquisitionSources2 for Closing {
	fn applied(&self) -> Result<Option<LogId<u32>>, SourceClosed2> {
		let n = self.calls.get();
		self.calls.set(n + 1);
		// Four entries / two per batch: initial + two terminal samples precede closure.
		if n == 3 {
			self.last
		} else {
			Ok(Some(self.cut))
		}
	}
	fn retention(&self) -> RetentionStatus {
		let n = self.retention_calls.get();
		self.retention_calls.set(n + 1);
		if n == 3 { self.status } else { full() }
	}
}
#[tokio::test(flavor = "current_thread")]
async fn run_exports_only_after_confirmed_mark_and_idempotent_rerun() {
	let (pin, dep, entries, fixtures) = context();
	let _scope = Root2FixtureScope::install(fixtures);
	let expected = oracle(&entries, dep, &pin);
	let (_d, mut m) = marks();
	for _ in 0..2 {
		let mut reader = store(entries.clone()).await;
		let output = run(
			&mut reader,
			&Sources(entries[3].log_id),
			caps(),
			dep,
			&pin,
			&mut m,
		)
		.await
		.unwrap();
		assert_eq!(
			encode_authority2(output.state()).unwrap(),
			encode_authority2(expected.last().unwrap()).unwrap()
		);
		assert_eq!(output.mark(), mark_of2(expected.last().unwrap()).unwrap());
		assert_eq!(output.report().entries(), 4);
		assert_eq!(m.durable(), Some(Baseline2::Mark(output.mark())));
	}
}
#[tokio::test(flavor = "current_thread")]
async fn closure_failures_preserve_prior_bytes_and_allow_advancement() {
	let (pin, dep, entries, fixtures) = context();
	let _scope = Root2FixtureScope::install(fixtures);
	let cut = entries[3].log_id;
	for (last, want) in [
		(Err(SourceClosed2), Some(ClosureError2::SourceClosed)),
		(Ok(None), Some(ClosureError2::Regressed)),
		(Ok(Some(entries[2].log_id)), Some(ClosureError2::Regressed)),
		(
			Ok(Some(LogId::new(CommittedLeaderId::new(9, 1), cut.index))),
			Some(ClosureError2::Conflict),
		),
		(Ok(Some(entry(9).log_id)), None),
	] {
		let (d, mut m) = marks();
		let before = bytes(&d);
		let mut reader = store(entries.clone()).await;
		let mut sources = Closing::new(cut);
		sources.last = last;
		let result = run(&mut reader, &sources, caps(), dep, &pin, &mut m).await;
		if let Some(want) = want {
			assert!(matches!(result,Err(ReplayError2::Closure(got)) if got==want));
			assert_eq!(bytes(&d), before);
			assert_eq!(m.durable(), Some(Baseline2::Provisioned));
		} else {
			assert!(result.is_ok());
		}
	}
	for mode in 0..3 {
		let (d, mut m) = marks();
		let before = bytes(&d);
		let mut reader = store(entries.clone()).await;
		let mut sources = Closing::new(cut);
		match mode {
			0 => sources.status.configured = ControlRetention::Compacting,
			1 => sources.status.history = HistoryCheck::NotChecked,
			_ => {
				sources.status.violation =
					Some(crate::consensus::retention::RetentionViolation::Purge)
			}
		}
		assert!(matches!(
			run(&mut reader, &sources, caps(), dep, &pin, &mut m).await,
			Err(ReplayError2::Closure(ClosureError2::Retention))
		));
		assert_eq!(bytes(&d), before);
	}
}
#[tokio::test(flavor = "current_thread")]
async fn baseline_unavailable_precedes_acquisition_and_binding_precedes_mark() {
	let (pin, dep, entries, mut fixtures) = context();
	if let Command2::Bootstrap { length, .. } = &mut fixtures[0].3 {
		*length += 1;
	}
	let cmd = fixtures.remove(0).3;
	fixtures.push(fixture(&entries[2], cmd, dep));
	let _scope = Root2FixtureScope::install(fixtures);
	let (d, mut m) = marks();
	let before = bytes(&d);
	let mut reader = store(entries.clone()).await;
	assert!(matches!(
		run(
			&mut reader,
			&Sources(entries[3].log_id),
			caps(),
			dep,
			&pin,
			&mut m
		)
		.await,
		Err(ReplayError2::Binding(_))
	));
	assert_eq!(bytes(&d), before);
	let vacant = TestDir::new("wendelon-replay-unprovisioned");
	std::fs::set_permissions(vacant.as_ref(), std::fs::Permissions::from_mode(0o700)).unwrap();
	let (mut m, _) = MarkStore2::open(vacant.as_ref()).unwrap();
	let sources = Closing::new(entries[3].log_id);
	assert!(matches!(
		run(&mut reader, &sources, caps(), dep, &pin, &mut m).await,
		Err(ReplayError2::Baseline(BaselineError2::Unavailable))
	));
	assert_eq!(sources.calls.get(), 0);
}
#[tokio::test(flavor = "current_thread")]
async fn mark_faults_do_not_export_and_poison_is_observed() {
	let (pin, dep, entries, fixtures) = context();
	let _scope = Root2FixtureScope::install(fixtures);
	for step in [
		MarkStep2::TmpCreate,
		MarkStep2::TmpWrite,
		MarkStep2::TmpSync,
		MarkStep2::Rename,
		MarkStep2::DirSync,
	] {
		let (d, mut m) = marks();
		let prior = bytes(&d);
		let mut reader = store(entries.clone()).await;
		let _plan = seam::plan(&[(step, 1, seam::Fault::Before)], None);
		assert!(matches!(
			run(
				&mut reader,
				&Sources(entries[3].log_id),
				caps(),
				dep,
				&pin,
				&mut m
			)
			.await,
			Err(ReplayError2::Mark(_))
		));
		assert_eq!(m.durable(), None);
		if step != MarkStep2::DirSync {
			assert_eq!(bytes(&d), prior);
		}
		let sources = Closing::new(entries[3].log_id);
		assert!(matches!(
			run(&mut reader, &sources, caps(), dep, &pin, &mut m).await,
			Err(ReplayError2::Baseline(BaselineError2::Unavailable))
		));
		assert_eq!(sources.calls.get(), 0);
	}
}
#[test]
fn mark_outcome_mapping_and_confirmation_are_exact() {
	let m = RecoveryMark2 {
		generation: [1; 16],
		cluster: [2; 16],
		cut: Log2 {
			term: 1,
			node: 1,
			index: 3,
		},
		fingerprint: [3; 32],
		command: [4; 32],
		state: [5; 32],
	};
	for out in [
		MarkWrite2::Durable(Baseline2::Mark(m)),
		MarkWrite2::Unchanged(Baseline2::Mark(m)),
	] {
		assert!(confirm_mark(out, Some(Baseline2::Mark(m)), m).is_ok());
		assert!(matches!(
			confirm_mark(out, None, m),
			Err(ReplayError2::Refused(MarkRefusal2::NotConfirmed))
		));
	}
	assert!(matches!(
		confirm_mark(
			MarkWrite2::Unchanged(Baseline2::Provisioned),
			Some(Baseline2::Mark(m)),
			m
		),
		Err(ReplayError2::Refused(MarkRefusal2::NotConfirmed))
	));
	for (e, want) in [
		(MarkIoError2::Identity, MarkRefusal2::Identity),
		(MarkIoError2::Regressed, MarkRefusal2::Regressed),
		(MarkIoError2::Conflict, MarkRefusal2::Conflict),
	] {
		assert!(matches!(map_mark_error(e),ReplayError2::Refused(got) if got==want));
	}
	for e in [
		MarkIoError2::Busy,
		MarkIoError2::NotDirectory,
		MarkIoError2::InsecureDir,
		MarkIoError2::NotRegular {
			step: MarkStep2::MarkRead,
		},
		MarkIoError2::Io {
			step: MarkStep2::TmpWrite,
			errno: Some(5),
		},
		MarkIoError2::Uncertain {
			step: MarkStep2::Rename,
		},
		MarkIoError2::Poisoned {
			step: MarkStep2::DirSync,
		},
		MarkIoError2::DirectoryChanged,
		MarkIoError2::Interference,
		MarkIoError2::AlreadyProvisioned,
		MarkIoError2::NotProvisioned,
		MarkIoError2::Corrupt,
		MarkIoError2::Invalid(
			crate::storage::mutation_visibility::root2::recovery::FieldError2::Cut,
		),
	] {
		assert!(matches!(map_mark_error(e),ReplayError2::Mark(got) if got==e));
	}
}
struct PendingReader(WendelonLogStore, usize);
impl RaftLogReader<WendelonTypeConfig> for PendingReader {
	async fn try_get_log_entries<
		RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend,
	>(
		&mut self,
		range: RB,
	) -> Result<Vec<E>, openraft::StorageError<u32>> {
		self.1 += 1;
		if self.1 == 3 {
			std::future::pending::<()>().await;
		}
		self.0.try_get_log_entries(range).await
	}
}
#[tokio::test(flavor = "current_thread")]
async fn cancelled_run_after_provisional_fold_keeps_mark_and_clears_fixture() {
	let (pin, dep, entries, fixtures) = context();
	let (p, fp) = fields(&entries[2]);
	let (d, mut m) = marks();
	let prior = bytes(&d);
	let mut reader = PendingReader(store(entries.clone()).await, 0);
	{
		let mut future = Box::pin(async {
			let _scope = Root2FixtureScope::install(fixtures);
			run(
				&mut reader,
				&Sources(entries[3].log_id),
				caps(),
				dep,
				&pin,
				&mut m,
			)
			.await
		});
		tokio::select! {biased; _=&mut future=>panic!("reader should suspend"), _=tokio::task::yield_now()=>{}}
	}
	assert_eq!(reader.1, 3);
	assert_eq!(bytes(&d), prior);
	assert_eq!(m.durable(), Some(Baseline2::Provisioned));
	assert!(
		crate::storage::mutation_visibility::root2::committed::fixture::command(p, fp).is_none()
	);
}
