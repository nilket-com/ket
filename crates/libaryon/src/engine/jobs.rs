//! Bounded, owner-scoped subprocess jobs.

use super::event_subscribers::{BoundedStatus, SubscriberOutcome};
use super::request::{InternalEvent, InternalSender};
use super::runtime_registry::{ActionLease, InvocationGate};
use crate::core::{JobId, RegistryOwnerId};
use std::collections::{BTreeMap, VecDeque};
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub(crate) const MAX_JOBS_PER_OWNER: usize = 16;
pub(crate) const MAX_JOBS_TOTAL: usize = 64;
pub(crate) const MAX_RUNNING_JOBS: usize = 16;
pub(crate) const MAX_JOB_CAPTURE_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAX_JOB_BUFFER_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_JOB_RUNTIME: Duration = Duration::from_secs(120);
pub(crate) const JOB_KILL_GRACE: Duration = Duration::from_secs(2);
pub(crate) const JOB_DRAIN_DEADLINE: Duration = Duration::from_secs(2);
pub(crate) const JOB_SHUTDOWN_REAP_BUDGET: Duration = Duration::from_millis(50);
pub(crate) const MAX_JOB_ARGS: usize = 256;
pub(crate) const MAX_JOB_ARG_BYTES: usize = 128 * 1024;
pub(crate) const MAX_JOB_PROGRAM_BYTES: usize = 4 * 1024;
pub(crate) const MAX_JOB_CWD_BYTES: usize = 4 * 1024;
pub(crate) const MAX_JOB_STDIN_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAX_JOB_ERROR_BYTES: usize = 4 * 1024;
pub(crate) const MAX_JOB_DELIVERY_STEPS: usize =
	super::event_subscribers::MAX_SUBSCRIBER_STEPS_PER_DRAIN;

#[derive(Debug)]
pub(crate) struct JobSpec {
	pub(crate) program: PathBuf,
	pub(crate) args: Vec<OsString>,
	pub(crate) cwd: Option<PathBuf>,
	pub(crate) stdin: Option<Vec<u8>>,
	pub(crate) timeout: Duration,
	/// Ordinary callers supply their callback up front. Rune reservations leave
	/// this empty until their `Function` can safely be retained after VM entry.
	pub(crate) completion: Option<ActionLease>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JobSpawnError {
	RetiredOwner(RegistryOwnerId),
	ActionOwnerMismatch,
	TooManyOwnerJobs,
	TooManySessionJobs,
	JobIdsExhausted,
	TooManyArguments,
	ArgumentsTooLarge,
	ProgramTooLarge,
	WorkingDirectoryTooLarge,
	StdinTooLarge,
	BufferBudgetExhausted,
	RuntimeUnavailable,
}

impl fmt::Display for JobSpawnError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::RetiredOwner(owner) => write!(formatter, "Runtime owner {owner} is retired"),
			Self::ActionOwnerMismatch => {
				formatter.write_str("Job completion action belongs to another owner")
			}
			Self::TooManyOwnerJobs => formatter.write_str("Job owner concurrency limit reached"),
			Self::TooManySessionJobs => formatter.write_str("Session job limit reached"),
			Self::JobIdsExhausted => formatter.write_str("Job IDs are exhausted"),
			Self::TooManyArguments => formatter.write_str("Job has too many arguments"),
			Self::ArgumentsTooLarge => formatter.write_str("Job arguments are too large"),
			Self::ProgramTooLarge => formatter.write_str("Job program path is too large"),
			Self::WorkingDirectoryTooLarge => {
				formatter.write_str("Job working directory is too large")
			}
			Self::StdinTooLarge => formatter.write_str("Job stdin is too large"),
			Self::BufferBudgetExhausted => formatter.write_str("Job buffer budget is exhausted"),
			Self::RuntimeUnavailable => formatter.write_str("Job runtime is unavailable"),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobCancelError {
	UnknownJob,
	AlreadyFinished,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobStatus {
	Queued,
	Running,
	CancellationPending,
	Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UnknownJob;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobErrorKind {
	Spawn,
	Io,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BoundedJobError {
	pub(crate) kind: JobErrorKind,
	pub(crate) message: String,
}

impl BoundedJobError {
	fn new(kind: JobErrorKind, message: impl fmt::Display) -> Self {
		struct CappedFormatter {
			message: String,
			truncated: bool,
		}

		impl fmt::Write for CappedFormatter {
			fn write_str(&mut self, value: &str) -> fmt::Result {
				if self.truncated {
					return Err(fmt::Error);
				}
				let remaining = MAX_JOB_ERROR_BYTES.saturating_sub(self.message.len());
				if value.len() <= remaining {
					self.message.push_str(value);
					return Ok(());
				}
				let mut take = remaining;
				while take > 0 && !value.is_char_boundary(take) {
					take -= 1;
				}
				self.message.push_str(&value[..take]);
				self.truncated = true;
				Err(fmt::Error)
			}
		}

		let mut formatter = CappedFormatter {
			message: String::with_capacity(MAX_JOB_ERROR_BYTES),
			truncated: false,
		};
		let _ = fmt::write(&mut formatter, format_args!("{message}"));
		let mut message = formatter.message;
		if formatter.truncated {
			let mut end = MAX_JOB_ERROR_BYTES.saturating_sub('…'.len_utf8());
			end = end.min(message.len());
			while end > 0 && !message.is_char_boundary(end) {
				end -= 1;
			}
			message.truncate(end);
			message.push('…');
		}
		Self { kind, message }
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JobOutcome {
	Exited { status: i32 },
	Signaled { signal: Option<i32> },
	TimedOut,
	Cancelled,
	SpawnFailed { error: BoundedJobError },
}

struct JobBufferBudget {
	charged: AtomicUsize,
}

impl JobBufferBudget {
	fn new() -> Arc<Self> {
		Arc::new(Self {
			charged: AtomicUsize::new(0),
		})
	}

	fn reserve(self: &Arc<Self>, bytes: usize) -> Option<JobBufferReservation> {
		let mut charged = self.charged.load(Ordering::Acquire);
		loop {
			let next = charged.checked_add(bytes)?;
			if next > MAX_JOB_BUFFER_BYTES {
				return None;
			}
			match self.charged.compare_exchange_weak(
				charged,
				next,
				Ordering::AcqRel,
				Ordering::Acquire,
			) {
				Ok(_) => {
					return Some(JobBufferReservation {
						budget: self.clone(),
						bytes,
					});
				}
				Err(actual) => charged = actual,
			}
		}
	}

	fn charged(&self) -> usize {
		self.charged.load(Ordering::Acquire)
	}
}

struct JobBufferReservation {
	budget: Arc<JobBufferBudget>,
	bytes: usize,
}

impl Drop for JobBufferReservation {
	fn drop(&mut self) {
		let previous = self.budget.charged.fetch_sub(self.bytes, Ordering::AcqRel);
		debug_assert!(previous >= self.bytes, "job buffer accounting underflowed");
	}
}

struct ChargedStdin {
	bytes: Vec<u8>,
	_reservation: JobBufferReservation,
}

struct CaptureChunk {
	bytes: Box<[u8]>,
	_reservation: JobBufferReservation,
}

#[derive(Default)]
pub(crate) struct JobCapture {
	chunks: Vec<CaptureChunk>,
	len: usize,
	pub(crate) truncated: bool,
}

impl fmt::Debug for JobCapture {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter
			.debug_struct("JobCapture")
			.field("len", &self.len)
			.field("truncated", &self.truncated)
			.finish()
	}
}

impl JobCapture {
	fn push(&mut self, budget: &Arc<JobBufferBudget>, bytes: &[u8]) {
		let remaining = MAX_JOB_CAPTURE_BYTES.saturating_sub(self.len);
		let take = remaining.min(bytes.len());
		if take > 0 {
			if let Some(reservation) = budget.reserve(take) {
				self.chunks.push(CaptureChunk {
					// `Box<[u8]>` is exact-sized, so the reservation taken before
					// allocation is also the allocation's retained capacity.
					bytes: Box::from(&bytes[..take]),
					_reservation: reservation,
				});
				self.len += take;
			} else {
				self.truncated = true;
			}
		}
		if take < bytes.len() {
			self.truncated = true;
		}
	}

	pub(crate) fn len(&self) -> usize {
		self.len
	}

	pub(crate) fn chunks(&self) -> impl Iterator<Item = &[u8]> {
		self.chunks.iter().map(|chunk| chunk.bytes.as_ref())
	}

	#[cfg(test)]
	pub(crate) fn to_vec(&self) -> Vec<u8> {
		let mut output = Vec::with_capacity(self.len);
		for chunk in self.chunks() {
			output.extend_from_slice(chunk);
		}
		output
	}
}

#[derive(Debug)]
pub(crate) struct JobCompletion {
	pub(crate) job: JobId,
	pub(crate) owner: RegistryOwnerId,
	pub(crate) outcome: JobOutcome,
	// Completion consumers can retain these captures for the duration of their
	// callback without flattening them into another native buffer.
	pub(crate) stdout: Arc<JobCapture>,
	pub(crate) stderr: Arc<JobCapture>,
	pub(crate) runtime: Duration,
}

#[derive(Debug, Clone)]
pub(crate) struct JobInvocation {
	job: JobId,
	lease: ActionLease,
}

/// An admitted but not yet runnable job.  This is intentionally only usable by
/// the host bridge: it lets Rune retain a completion Function after its active
/// borrow has ended without creating a child in the meantime.
#[derive(Debug, Clone, Copy)]
pub(crate) struct JobReservation {
	id: JobId,
	owner: RegistryOwnerId,
}

impl JobReservation {
	pub(crate) const fn id(self) -> JobId {
		self.id
	}
}

impl JobInvocation {
	pub(crate) const fn job(&self) -> JobId {
		self.job
	}

	pub(crate) const fn owner(&self) -> RegistryOwnerId {
		self.lease.owner()
	}

	pub(crate) fn action<T: 'static>(&self) -> Option<&T> {
		self.lease.downcast_ref()
	}
}

pub(crate) trait JobDispatcher<C> {
	fn owner_is_live(&self, owner: RegistryOwnerId) -> bool;
	fn complete(
		&mut self,
		context: &mut C,
		invocation: JobInvocation,
		completion: &JobCompletion,
	) -> SubscriberOutcome;
}

pub(crate) struct UnavailableJobDispatcher;

impl<C> JobDispatcher<C> for UnavailableJobDispatcher {
	fn owner_is_live(&self, _owner: RegistryOwnerId) -> bool {
		true
	}

	fn complete(
		&mut self,
		_context: &mut C,
		_invocation: JobInvocation,
		_completion: &JobCompletion,
	) -> SubscriberOutcome {
		SubscriberOutcome::failed(BoundedStatus::fixed(
			"Runtime job dispatcher is unavailable",
		))
	}
}

struct PreparedJob {
	program: PathBuf,
	args: Vec<OsString>,
	cwd: PathBuf,
	stdin: Option<ChargedStdin>,
	timeout: Duration,
}

enum SupervisorControl {
	Terminate,
	Shutdown,
}

enum CandidateOutcome {
	Exited { status: i32 },
	Signaled { signal: Option<i32> },
	TimedOut,
	SpawnFailed(BoundedJobError),
}

struct SupervisorReport {
	candidate: CandidateOutcome,
	stdout: JobCapture,
	stderr: JobCapture,
	runtime: Duration,
}

struct RunningJob {
	control: mpsc::Sender<SupervisorControl>,
	report: Arc<Mutex<Option<SupervisorReport>>>,
	join: Option<JoinHandle<()>>,
	fixed: Option<JobOutcome>,
}

enum JobState {
	Reserved(Option<PreparedJob>),
	Queued(PreparedJob),
	Running(RunningJob),
	Deliverable(JobCompletion),
}

struct JobRecord {
	owner: RegistryOwnerId,
	lease: Option<ActionLease>,
	state: JobState,
}

pub(crate) struct JobRuntime {
	next_id: JobId,
	jobs: BTreeMap<JobId, JobRecord>,
	queued: VecDeque<JobId>,
	running: usize,
	deliverable: VecDeque<JobId>,
	pending_delivery: Option<VecDeque<JobId>>,
	retired_supervisors: Vec<JoinHandle<()>>,
	budget: Arc<JobBufferBudget>,
	internal: Option<InternalSender>,
	stop: Arc<AtomicBool>,
	default_cwd: PathBuf,
}

impl Default for JobRuntime {
	fn default() -> Self {
		Self::new()
	}
}

impl JobRuntime {
	pub(crate) fn new() -> Self {
		Self {
			next_id: JobId::new(1),
			jobs: BTreeMap::new(),
			queued: VecDeque::new(),
			running: 0,
			deliverable: VecDeque::new(),
			pending_delivery: None,
			retired_supervisors: Vec::new(),
			budget: JobBufferBudget::new(),
			internal: None,
			stop: Arc::new(AtomicBool::new(false)),
			default_cwd: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
		}
	}

	pub(crate) fn attach_internal_sender(&mut self, internal: InternalSender) {
		self.internal = Some(internal);
	}

	pub(crate) fn spawn(
		&mut self,
		owner: RegistryOwnerId,
		owner_is_live: bool,
		spec: JobSpec,
	) -> Result<JobId, JobSpawnError> {
		let lease = spec
			.completion
			.clone()
			.ok_or(JobSpawnError::RuntimeUnavailable)?;
		if lease.owner() != owner {
			return Err(JobSpawnError::ActionOwnerMismatch);
		}
		let reservation = self.reserve_job(owner, owner_is_live, spec)?;
		match self.commit_reserved_job(reservation, lease) {
			Ok(id) => Ok(id),
			Err(error) => {
				self.discard_reservation(reservation);
				Err(error)
			}
		}
	}

	pub(crate) fn reserve_job(
		&mut self,
		owner: RegistryOwnerId,
		owner_is_live: bool,
		spec: JobSpec,
	) -> Result<JobReservation, JobSpawnError> {
		if !owner_is_live {
			return Err(JobSpawnError::RetiredOwner(owner));
		}
		if self.internal.is_none() || self.stop.load(Ordering::Acquire) {
			return Err(JobSpawnError::RuntimeUnavailable);
		}
		if self.jobs.len() >= MAX_JOBS_TOTAL {
			return Err(JobSpawnError::TooManySessionJobs);
		}
		if self.jobs.values().filter(|job| job.owner == owner).count() >= MAX_JOBS_PER_OWNER {
			return Err(JobSpawnError::TooManyOwnerJobs);
		}
		if self.next_id.get() == u64::MAX {
			return Err(JobSpawnError::JobIdsExhausted);
		}
		validate_spec(&spec)?;
		let stdin = match spec.stdin {
			Some(bytes) => {
				let reservation = self
					.budget
					.reserve(bytes.capacity())
					.ok_or(JobSpawnError::BufferBudgetExhausted)?;
				Some(ChargedStdin {
					bytes,
					_reservation: reservation,
				})
			}
			None => None,
		};
		let id = self.next_id;
		self.next_id = self.next_id.next();
		let prepared = PreparedJob {
			program: spec.program,
			args: spec.args,
			cwd: spec.cwd.unwrap_or_else(|| self.default_cwd.clone()),
			stdin,
			timeout: spec.timeout.min(MAX_JOB_RUNTIME),
		};
		self.jobs.insert(
			id,
			JobRecord {
				owner,
				lease: None,
				state: JobState::Reserved(Some(prepared)),
			},
		);
		Ok(JobReservation { id, owner })
	}

	pub(crate) fn commit_reserved_job(
		&mut self,
		reservation: JobReservation,
		lease: ActionLease,
	) -> Result<JobId, JobSpawnError> {
		if lease.owner() != reservation.owner {
			return Err(JobSpawnError::ActionOwnerMismatch);
		}
		let record = self
			.jobs
			.get_mut(&reservation.id)
			.ok_or(JobSpawnError::RuntimeUnavailable)?;
		if record.owner != reservation.owner {
			return Err(JobSpawnError::RuntimeUnavailable);
		}
		let JobState::Reserved(prepared) = &mut record.state else {
			return Err(JobSpawnError::RuntimeUnavailable);
		};
		let Some(prepared) = prepared.take() else {
			return Err(JobSpawnError::RuntimeUnavailable);
		};
		record.lease = Some(lease);
		record.state = JobState::Queued(prepared);
		self.queued.push_back(reservation.id);
		self.start_queued();
		Ok(reservation.id)
	}

	pub(crate) fn discard_reservation(&mut self, reservation: JobReservation) {
		if self.jobs.get(&reservation.id).is_some_and(|record| {
			record.owner == reservation.owner && matches!(record.state, JobState::Reserved(_))
		}) {
			self.jobs.remove(&reservation.id);
		}
	}

	pub(crate) fn cancel(
		&mut self,
		owner: RegistryOwnerId,
		id: JobId,
	) -> Result<(), JobCancelError> {
		let Some(record) = self.jobs.get_mut(&id) else {
			return Err(JobCancelError::UnknownJob);
		};
		if record.owner != owner {
			return Err(JobCancelError::UnknownJob);
		}
		match &mut record.state {
			JobState::Reserved(_) => {
				self.jobs.remove(&id);
				Ok(())
			}
			JobState::Queued(_) => {
				self.queued.retain(|queued| *queued != id);
				self.jobs.remove(&id);
				Ok(())
			}
			JobState::Running(running) if running.fixed.is_none() => {
				running.fixed = Some(JobOutcome::Cancelled);
				let _ = running.control.send(SupervisorControl::Terminate);
				Ok(())
			}
			JobState::Running(_) | JobState::Deliverable(_) => Err(JobCancelError::AlreadyFinished),
		}
	}

	pub(crate) fn status(
		&self,
		owner: RegistryOwnerId,
		id: JobId,
	) -> Result<JobStatus, UnknownJob> {
		let record = self.jobs.get(&id).ok_or(UnknownJob)?;
		if record.owner != owner {
			return Err(UnknownJob);
		}
		Ok(match &record.state {
			JobState::Reserved(_) => JobStatus::Queued,
			JobState::Queued(_) => JobStatus::Queued,
			JobState::Running(RunningJob { fixed: Some(_), .. }) => JobStatus::CancellationPending,
			JobState::Running(_) => JobStatus::Running,
			JobState::Deliverable(_) => JobStatus::Completed,
		})
	}

	pub(crate) fn cancel_owner(&mut self, owner: RegistryOwnerId) {
		let ids: Vec<_> = self
			.jobs
			.iter()
			.filter_map(|(id, record)| (record.owner == owner).then_some(*id))
			.collect();
		for id in ids {
			let _ = self.cancel(owner, id);
			if self
				.jobs
				.get(&id)
				.is_some_and(|record| matches!(record.state, JobState::Deliverable(_)))
			{
				self.jobs.remove(&id);
				self.deliverable.retain(|queued| *queued != id);
				if let Some(pending) = &mut self.pending_delivery {
					pending.retain(|queued| *queued != id);
				}
			}
		}
	}

	pub(crate) fn handle_supervisor_report(&mut self, id: JobId, owner_is_live: bool) {
		let Some(mut record) = self.jobs.remove(&id) else {
			return;
		};
		let JobState::Running(mut running) = record.state else {
			self.jobs.insert(id, record);
			return;
		};
		let Some(report) = running.report.lock().expect("job report lock").take() else {
			record.state = JobState::Running(running);
			self.jobs.insert(id, record);
			return;
		};
		self.running = self.running.saturating_sub(1);
		if let Some(join) = running.join.take() {
			if join.is_finished() {
				let _ = join.join();
			} else {
				self.retired_supervisors.push(join);
			}
		}
		self.reap_finished_supervisors();
		let outcome = running
			.fixed
			.unwrap_or_else(|| candidate_outcome(report.candidate));
		if owner_is_live && outcome != JobOutcome::Cancelled {
			record.state = JobState::Deliverable(JobCompletion {
				job: id,
				owner: record.owner,
				outcome,
				stdout: Arc::new(report.stdout),
				stderr: Arc::new(report.stderr),
				runtime: report.runtime,
			});
			self.jobs.insert(id, record);
			self.deliverable.push_back(id);
		} // otherwise record/report drop here, releasing every buffer charge.
		self.start_queued();
	}

	pub(crate) fn owner(&self, id: JobId) -> Option<RegistryOwnerId> {
		self.jobs.get(&id).map(|record| record.owner)
	}

	pub(crate) fn has_pending_delivery(&self) -> bool {
		self.pending_delivery.is_some()
	}

	pub(crate) fn capture_delivery_batch(&mut self) {
		if self.pending_delivery.is_none() && !self.deliverable.is_empty() {
			self.pending_delivery = Some(std::mem::take(&mut self.deliverable));
		}
	}

	pub(crate) fn drain_deliveries<C, D>(
		&mut self,
		context: &mut C,
		gate: &InvocationGate,
		dispatcher: &mut D,
		steps: &mut usize,
	) -> SubscriberOutcome
	where
		D: JobDispatcher<C> + ?Sized,
	{
		let mut merged = SubscriberOutcome::delivered();
		while *steps < MAX_JOB_DELIVERY_STEPS {
			let id = match self.pending_delivery.as_mut().and_then(VecDeque::pop_front) {
				Some(id) => id,
				None => {
					self.pending_delivery = None;
					break;
				}
			};
			*steps += 1;
			let Some(record) = self.jobs.remove(&id) else {
				continue;
			};
			let JobState::Deliverable(completion) = record.state else {
				continue;
			};
			let invocation = JobInvocation {
				job: id,
				lease: record
					.lease
					.expect("deliverable jobs have completion leases"),
			};
			let owner = invocation.owner();
			let owner_is_live = dispatcher.owner_is_live(owner);
			let outcome = match gate.invoke_guarded(owner, owner_is_live, || {
				dispatcher.complete(context, invocation, &completion)
			}) {
				Ok(outcome) => outcome,
				Err(error) => {
					SubscriberOutcome::failed(BoundedStatus::new(error.to_string()).unwrap_or_else(
						|_| BoundedStatus::fixed("Runtime job completion invocation failed"),
					))
				}
			};
			merged.needs_render |= outcome.needs_render;
			if outcome.foreground_status.is_some() {
				merged.foreground_status = outcome.foreground_status;
			}
		}
		merged
	}

	pub(crate) fn take_next_delivery(&mut self) -> Option<(JobInvocation, JobCompletion)> {
		loop {
			let id = match self.pending_delivery.as_mut().and_then(VecDeque::pop_front) {
				Some(id) => id,
				None => {
					self.pending_delivery = None;
					return None;
				}
			};
			let Some(record) = self.jobs.remove(&id) else {
				continue;
			};
			let JobState::Deliverable(completion) = record.state else {
				continue;
			};
			return Some((
				JobInvocation {
					job: id,
					lease: record
						.lease
						.expect("deliverable jobs have completion leases"),
				},
				completion,
			));
		}
	}

	pub(crate) fn charged_buffer_bytes(&self) -> usize {
		self.budget.charged()
	}

	#[cfg(test)]
	pub(crate) fn is_logically_cancelled(&self, id: JobId) -> bool {
		self.jobs.get(&id).is_none_or(|record| {
			matches!(
				&record.state,
				JobState::Running(RunningJob {
					fixed: Some(JobOutcome::Cancelled),
					..
				})
			)
		})
	}

	#[cfg(test)]
	pub(crate) fn is_running_unfixed(&self, id: JobId) -> bool {
		self.jobs.get(&id).is_some_and(|record| {
			matches!(
				&record.state,
				JobState::Running(RunningJob { fixed: None, .. })
			)
		})
	}

	fn start_queued(&mut self) {
		self.reap_finished_supervisors();
		while self.running < MAX_RUNNING_JOBS {
			let Some(id) = self.queued.pop_front() else {
				break;
			};
			let Some(record) = self.jobs.get_mut(&id) else {
				continue;
			};
			let JobState::Queued(prepared) = std::mem::replace(
				&mut record.state,
				JobState::Deliverable(empty_completion(id, record.owner)),
			) else {
				continue;
			};
			let report = Arc::new(Mutex::new(None));
			let (tx_control, rx_control) = mpsc::channel();
			let Some(internal) = self.internal.clone() else {
				break;
			};
			let worker_report = report.clone();
			let budget = self.budget.clone();
			let stop = self.stop.clone();
			let spawn = thread::Builder::new()
				.name(format!("baryon-job-{}", id.get()))
				.spawn(move || {
					let result = supervise(prepared, rx_control, budget);
					*worker_report.lock().expect("job report lock") = Some(result);
					send_completion(internal, stop, id);
				});
			match spawn {
				Ok(join) => {
					record.state = JobState::Running(RunningJob {
						control: tx_control,
						report,
						join: Some(join),
						fixed: None,
					});
					self.running += 1;
				}
				Err(error) => {
					let completion = JobCompletion {
						job: id,
						owner: record.owner,
						outcome: JobOutcome::SpawnFailed {
							error: BoundedJobError::new(JobErrorKind::Spawn, error),
						},
						stdout: Arc::new(JobCapture::default()),
						stderr: Arc::new(JobCapture::default()),
						runtime: Duration::ZERO,
					};
					record.state = JobState::Deliverable(completion);
					self.deliverable.push_back(id);
					if let Some(internal) = &self.internal {
						let _ = internal.try_send(InternalEvent::DrainEvents);
					}
				}
			}
		}
	}

	pub(crate) fn shutdown(&mut self) {
		if self.stop.swap(true, Ordering::AcqRel) {
			return;
		}
		self.queued.clear();
		let deadline = Instant::now() + JOB_SHUTDOWN_REAP_BUDGET;
		for record in self.jobs.values_mut() {
			if let JobState::Running(running) = &mut record.state {
				running.fixed = Some(JobOutcome::Cancelled);
				let _ = running.control.send(SupervisorControl::Shutdown);
			}
		}
		loop {
			self.reap_finished_supervisors();
			let mut unfinished = 0usize;
			for record in self.jobs.values_mut() {
				let JobState::Running(running) = &mut record.state else {
					continue;
				};
				if running.join.as_ref().is_some_and(JoinHandle::is_finished) {
					if let Some(join) = running.join.take() {
						let _ = join.join();
					}
				} else if running.join.is_some() {
					unfinished += 1;
				}
			}
			unfinished = unfinished.saturating_add(self.retired_supervisors.len());
			if unfinished == 0 || Instant::now() >= deadline {
				break;
			}
			thread::sleep(Duration::from_millis(1));
		}
		self.jobs.clear(); // unfinished JoinHandles detach here.
		self.retired_supervisors.clear();
		self.deliverable.clear();
		self.pending_delivery = None;
	}

	fn reap_finished_supervisors(&mut self) {
		let mut index = 0usize;
		while index < self.retired_supervisors.len() {
			if self.retired_supervisors[index].is_finished() {
				let join = self.retired_supervisors.swap_remove(index);
				let _ = join.join();
			} else {
				index += 1;
			}
		}
	}
}

impl Drop for JobRuntime {
	fn drop(&mut self) {
		self.shutdown();
	}
}

fn validate_spec(spec: &JobSpec) -> Result<(), JobSpawnError> {
	let arg_bytes = spec
		.args
		.iter()
		.try_fold(0usize, |total, arg| total.checked_add(os_len(arg)));
	validate_spec_limits(
		spec.args.len(),
		arg_bytes,
		os_len(spec.program.as_os_str()),
		spec.cwd.as_ref().map(|cwd| os_len(cwd.as_os_str())),
		spec.stdin.as_ref().map(Vec::len),
	)
}

pub(crate) fn validate_spec_limits(
	arg_count: usize,
	argument_bytes: Option<usize>,
	program_bytes: usize,
	cwd_bytes: Option<usize>,
	stdin_bytes: Option<usize>,
) -> Result<(), JobSpawnError> {
	if arg_count > MAX_JOB_ARGS {
		return Err(JobSpawnError::TooManyArguments);
	}
	if argument_bytes.is_none_or(|bytes| bytes > MAX_JOB_ARG_BYTES) {
		return Err(JobSpawnError::ArgumentsTooLarge);
	}
	if program_bytes > MAX_JOB_PROGRAM_BYTES {
		return Err(JobSpawnError::ProgramTooLarge);
	}
	if cwd_bytes.is_some_and(|bytes| bytes > MAX_JOB_CWD_BYTES) {
		return Err(JobSpawnError::WorkingDirectoryTooLarge);
	}
	if stdin_bytes.is_some_and(|bytes| bytes > MAX_JOB_STDIN_BYTES) {
		return Err(JobSpawnError::StdinTooLarge);
	}
	Ok(())
}

pub(crate) fn os_len_from_utf8(value: &str) -> usize {
	os_len(OsStr::new(value))
}

#[cfg(unix)]
fn os_len(value: &OsStr) -> usize {
	use std::os::unix::ffi::OsStrExt;
	value.as_bytes().len()
}

#[cfg(windows)]
fn os_len(value: &OsStr) -> usize {
	use std::os::windows::ffi::OsStrExt;
	value.encode_wide().count().saturating_mul(2)
}

#[cfg(not(any(unix, windows)))]
fn os_len(value: &OsStr) -> usize {
	value.to_string_lossy().len()
}

fn candidate_outcome(candidate: CandidateOutcome) -> JobOutcome {
	match candidate {
		CandidateOutcome::Exited { status } => JobOutcome::Exited { status },
		CandidateOutcome::Signaled { signal } => JobOutcome::Signaled { signal },
		CandidateOutcome::TimedOut => JobOutcome::TimedOut,
		CandidateOutcome::SpawnFailed(error) => JobOutcome::SpawnFailed { error },
	}
}

fn empty_completion(job: JobId, owner: RegistryOwnerId) -> JobCompletion {
	JobCompletion {
		job,
		owner,
		outcome: JobOutcome::Cancelled,
		stdout: Arc::new(JobCapture::default()),
		stderr: Arc::new(JobCapture::default()),
		runtime: Duration::ZERO,
	}
}

fn send_completion(internal: InternalSender, stop: Arc<AtomicBool>, id: JobId) {
	let mut event = InternalEvent::JobCompleted(id);
	loop {
		if stop.load(Ordering::Acquire) {
			return;
		}
		match internal.try_send(event) {
			Ok(()) => return,
			Err(mpsc::TrySendError::Full(returned)) => {
				event = returned;
				thread::sleep(Duration::from_millis(2));
			}
			Err(mpsc::TrySendError::Disconnected(_)) => return,
		}
	}
}

fn supervise(
	prepared: PreparedJob,
	control: mpsc::Receiver<SupervisorControl>,
	budget: Arc<JobBufferBudget>,
) -> SupervisorReport {
	#[cfg(target_os = "linux")]
	{
		supervise_linux(prepared, control, budget)
	}
	#[cfg(not(target_os = "linux"))]
	{
		supervise_portable(prepared, control, budget)
	}
}

fn command_for(prepared: &PreparedJob) -> Command {
	let mut command = Command::new(&prepared.program);
	command.args(&prepared.args).current_dir(&prepared.cwd);
	for (key, _) in std::env::vars_os() {
		if key.to_string_lossy().starts_with("BARYON_") {
			command.env_remove(key);
		}
	}
	command
		.stdin(if prepared.stdin.is_some() {
			Stdio::piped()
		} else {
			Stdio::null()
		})
		.stdout(Stdio::piped())
		.stderr(Stdio::piped());
	command
}

#[cfg(target_os = "linux")]
fn supervise_linux(
	mut prepared: PreparedJob,
	control: mpsc::Receiver<SupervisorControl>,
	budget: Arc<JobBufferBudget>,
) -> SupervisorReport {
	use std::os::fd::AsRawFd;
	use std::os::unix::process::{CommandExt, ExitStatusExt};

	let started = Instant::now();
	let mut command = command_for(&prepared);
	command.process_group(0);
	let mut child = match command.spawn() {
		Ok(child) => child,
		Err(error) => {
			return SupervisorReport {
				candidate: CandidateOutcome::SpawnFailed(BoundedJobError::new(
					JobErrorKind::Spawn,
					error,
				)),
				stdout: JobCapture::default(),
				stderr: JobCapture::default(),
				runtime: started.elapsed(),
			};
		}
	};
	let pgid = child.id() as i32;
	let mut stdin = child.stdin.take();
	let mut stdout = child.stdout.take();
	let mut stderr = child.stderr.take();
	for fd in [
		stdin.as_ref().map(AsRawFd::as_raw_fd),
		stdout.as_ref().map(AsRawFd::as_raw_fd),
		stderr.as_ref().map(AsRawFd::as_raw_fd),
	]
	.into_iter()
	.flatten()
	{
		set_nonblocking(fd);
	}
	let mut stdin_offset = 0usize;
	let mut stdout_capture = JobCapture::default();
	let mut stderr_capture = JobCapture::default();
	let mut exit_status = None;
	let mut timed_out = false;
	let mut termination_started = None;
	let mut killed = false;
	let mut drain_deadline = None;

	loop {
		while let Ok(signal) = control.try_recv() {
			match signal {
				SupervisorControl::Terminate if termination_started.is_none() => {
					signal_group(pgid, libc::SIGTERM);
					termination_started = Some(Instant::now());
				}
				SupervisorControl::Shutdown => {
					signal_group(pgid, libc::SIGKILL);
					killed = true;
					termination_started = Some(
						Instant::now()
							.checked_sub(JOB_KILL_GRACE)
							.unwrap_or_else(Instant::now),
					);
					drain_deadline = Some(Instant::now() + JOB_DRAIN_DEADLINE);
				}
				SupervisorControl::Terminate => {}
			}
		}
		if !timed_out && started.elapsed() >= prepared.timeout {
			timed_out = true;
			if termination_started.is_none() {
				signal_group(pgid, libc::SIGTERM);
				termination_started = Some(Instant::now());
			}
		}
		if !killed && termination_started.is_some_and(|at| at.elapsed() >= JOB_KILL_GRACE) {
			signal_group(pgid, libc::SIGKILL);
			killed = true;
			drain_deadline.get_or_insert(Instant::now() + JOB_DRAIN_DEADLINE);
		}
		if exit_status.is_none() {
			exit_status = child.try_wait().ok().flatten();
			if exit_status.is_some() {
				drain_deadline.get_or_insert(Instant::now() + JOB_DRAIN_DEADLINE);
			}
		}

		let mut close_stdin = false;
		if let (Some(handle), Some(charged)) = (&stdin, &prepared.stdin) {
			if stdin_offset < charged.bytes.len() {
				match write_fd(handle.as_raw_fd(), &charged.bytes[stdin_offset..]) {
					NonblockingWrite::Progress(written) => {
						stdin_offset = stdin_offset.saturating_add(written);
					}
					NonblockingWrite::Pending => {}
					NonblockingWrite::Closed => {
						close_stdin = true;
					}
				}
			}
			if stdin_offset >= charged.bytes.len() {
				close_stdin = true;
			}
		}
		if close_stdin {
			stdin = None;
			prepared.stdin = None;
		}
		read_fd(&mut stdout, &budget, &mut stdout_capture);
		read_fd(&mut stderr, &budget, &mut stderr_capture);

		if exit_status.is_some() && stdout.is_none() && stderr.is_none() {
			break;
		}
		if drain_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
			if stdout.take().is_some() {
				stdout_capture.truncated = true;
			}
			if stderr.take().is_some() {
				stderr_capture.truncated = true;
			}
			if exit_status.is_some() || killed {
				break;
			}
		}
		poll_fds(
			stdin.as_ref().map(AsRawFd::as_raw_fd),
			stdout.as_ref().map(AsRawFd::as_raw_fd),
			stderr.as_ref().map(AsRawFd::as_raw_fd),
		);
	}
	if exit_status.is_none() {
		exit_status = child.try_wait().ok().flatten();
		if exit_status.is_none() {
			let _ = thread::Builder::new()
				.name("baryon-job-reaper".to_string())
				.spawn(move || {
					let _ = child.wait();
				});
		}
	}

	let candidate = if timed_out {
		CandidateOutcome::TimedOut
	} else if let Some(status) = exit_status {
		match status.code() {
			Some(status) => CandidateOutcome::Exited { status },
			None => CandidateOutcome::Signaled {
				signal: status.signal(),
			},
		}
	} else {
		CandidateOutcome::Signaled { signal: None }
	};
	SupervisorReport {
		candidate,
		stdout: stdout_capture,
		stderr: stderr_capture,
		runtime: started.elapsed(),
	}
}

#[cfg(target_os = "linux")]
fn set_nonblocking(fd: std::os::fd::RawFd) {
	// SAFETY: fcntl only observes/updates flags for a live fd owned by this supervisor.
	let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
	if flags >= 0 {
		// SAFETY: the fd remains live and the flags came from F_GETFL.
		let _ = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
	}
}

#[cfg(target_os = "linux")]
fn signal_group(pgid: i32, signal: i32) {
	// SAFETY: negative pgid targets the child-owned process group, never an arbitrary pid.
	let _ = unsafe { libc::kill(-pgid, signal) };
}

#[cfg(target_os = "linux")]
enum NonblockingWrite {
	Progress(usize),
	Pending,
	Closed,
}

#[cfg(target_os = "linux")]
fn write_fd(fd: std::os::fd::RawFd, bytes: &[u8]) -> NonblockingWrite {
	loop {
		// SAFETY: bytes is valid for its length and fd is a live nonblocking child stdin.
		let result = unsafe { libc::write(fd, bytes.as_ptr().cast(), bytes.len()) };
		if let Ok(written) = usize::try_from(result) {
			return NonblockingWrite::Progress(written);
		}
		return match std::io::Error::last_os_error().kind() {
			std::io::ErrorKind::Interrupted => continue,
			std::io::ErrorKind::WouldBlock => NonblockingWrite::Pending,
			_ => NonblockingWrite::Closed,
		};
	}
}

#[cfg(target_os = "linux")]
fn read_fd<T: std::os::fd::AsRawFd>(
	handle: &mut Option<T>,
	budget: &Arc<JobBufferBudget>,
	capture: &mut JobCapture,
) {
	let Some(fd) = handle.as_ref().map(std::os::fd::AsRawFd::as_raw_fd) else {
		return;
	};
	let mut buffer = [0u8; 16 * 1024];
	loop {
		// SAFETY: buffer is writable and fd is a live nonblocking child output.
		let result = unsafe { libc::read(fd, buffer.as_mut_ptr().cast(), buffer.len()) };
		if result > 0 {
			capture.push(budget, &buffer[..result as usize]);
			continue;
		}
		if result == 0 {
			*handle = None;
		} else {
			match std::io::Error::last_os_error().kind() {
				std::io::ErrorKind::Interrupted => continue,
				std::io::ErrorKind::WouldBlock => {}
				_ => {
					capture.truncated = true;
					*handle = None;
				}
			}
		}
		break;
	}
}

#[cfg(target_os = "linux")]
fn poll_fds(stdin: Option<i32>, stdout: Option<i32>, stderr: Option<i32>) {
	let mut fds = Vec::with_capacity(3);
	if let Some(fd) = stdin {
		fds.push(libc::pollfd {
			fd,
			events: libc::POLLOUT,
			revents: 0,
		});
	}
	for fd in [stdout, stderr].into_iter().flatten() {
		fds.push(libc::pollfd {
			fd,
			events: libc::POLLIN,
			revents: 0,
		});
	}
	// SAFETY: fds points to initialized pollfd values for the duration of the call.
	let _ = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, 10) };
}

#[cfg(not(target_os = "linux"))]
fn supervise_portable(
	mut prepared: PreparedJob,
	control: mpsc::Receiver<SupervisorControl>,
	budget: Arc<JobBufferBudget>,
) -> SupervisorReport {
	use std::io::{Read, Write};

	fn capture_pipe<R: Read + Send + 'static>(
		mut pipe: R,
		budget: Arc<JobBufferBudget>,
	) -> mpsc::Receiver<JobCapture> {
		let (tx, rx) = mpsc::sync_channel(1);
		let fallback = tx.clone();
		let spawn = thread::Builder::new()
			.name("baryon-job-pipe".to_string())
			.spawn(move || {
				let mut capture = JobCapture::default();
				let mut bytes = [0u8; 16 * 1024];
				loop {
					match pipe.read(&mut bytes) {
						Ok(0) => break,
						Ok(read) => capture.push(&budget, &bytes[..read]),
						Err(_) => {
							capture.truncated = true;
							break;
						}
					}
				}
				let _ = tx.send(capture);
			});
		if spawn.is_err() {
			let _ = fallback.send(JobCapture {
				truncated: true,
				..JobCapture::default()
			});
		}
		rx
	}

	fn finish_capture(rx: &mpsc::Receiver<JobCapture>, deadline: Instant) -> JobCapture {
		let remaining = deadline.saturating_duration_since(Instant::now());
		rx.recv_timeout(remaining).unwrap_or_else(|_| JobCapture {
			truncated: true,
			..JobCapture::default()
		})
	}

	let started = Instant::now();
	let mut command = command_for(&prepared);
	let mut child = match command.spawn() {
		Ok(child) => child,
		Err(error) => {
			return SupervisorReport {
				candidate: CandidateOutcome::SpawnFailed(BoundedJobError::new(
					JobErrorKind::Spawn,
					error,
				)),
				stdout: JobCapture::default(),
				stderr: JobCapture::default(),
				runtime: started.elapsed(),
			};
		}
	};
	let stdout = capture_pipe(
		child.stdout.take().expect("piped child stdout"),
		budget.clone(),
	);
	let stderr = capture_pipe(
		child.stderr.take().expect("piped child stderr"),
		budget.clone(),
	);
	if let (Some(mut stdin), Some(charged)) = (child.stdin.take(), prepared.stdin.take()) {
		let _ = thread::Builder::new()
			.name("baryon-job-stdin".to_string())
			.spawn(move || {
				let _ = stdin.write_all(&charged.bytes);
			});
	}

	let mut timed_out = false;
	let candidate = loop {
		while let Ok(signal) = control.try_recv() {
			match signal {
				SupervisorControl::Terminate | SupervisorControl::Shutdown => {
					// Portable fallback can terminate only the direct child. A
					// descendant retaining a pipe is bounded by the capture deadline.
					let _ = child.kill();
				}
			}
		}
		if !timed_out && started.elapsed() >= prepared.timeout {
			timed_out = true;
			let _ = child.kill();
		}
		match child.try_wait() {
			Ok(Some(status)) => {
				break if timed_out {
					CandidateOutcome::TimedOut
				} else {
					status_candidate(status)
				};
			}
			Ok(None) => thread::sleep(Duration::from_millis(10)),
			Err(error) => {
				break CandidateOutcome::SpawnFailed(BoundedJobError::new(JobErrorKind::Io, error));
			}
		}
	};
	let drain_deadline = Instant::now() + JOB_DRAIN_DEADLINE;
	let stdout_capture = finish_capture(&stdout, drain_deadline);
	let stderr_capture = finish_capture(&stderr, drain_deadline);
	SupervisorReport {
		candidate,
		stdout: stdout_capture,
		stderr: stderr_capture,
		runtime: started.elapsed(),
	}
}

#[cfg(not(target_os = "linux"))]
fn status_candidate(status: std::process::ExitStatus) -> CandidateOutcome {
	status
		.code()
		.map_or(CandidateOutcome::Signaled { signal: None }, |status| {
			CandidateOutcome::Exited { status }
		})
}

#[cfg(test)]
mod tests {
	use super::super::request::EngineRequest;
	use super::*;

	#[derive(Debug, Clone)]
	struct FakeAction;

	fn spec(owner: RegistryOwnerId) -> JobSpec {
		JobSpec {
			program: PathBuf::from("/bin/true"),
			args: Vec::new(),
			cwd: None,
			stdin: None,
			timeout: Duration::from_secs(1),
			completion: Some(ActionLease::new(owner, FakeAction)),
		}
	}

	#[derive(Default)]
	struct RecordingDispatcher {
		completions: Vec<(JobOutcome, Vec<u8>, Vec<u8>)>,
	}

	impl JobDispatcher<()> for RecordingDispatcher {
		fn owner_is_live(&self, _owner: RegistryOwnerId) -> bool {
			true
		}

		fn complete(
			&mut self,
			_context: &mut (),
			_invocation: JobInvocation,
			completion: &JobCompletion,
		) -> SubscriberOutcome {
			self.completions.push((
				completion.outcome.clone(),
				completion.stdout.to_vec(),
				completion.stderr.to_vec(),
			));
			SubscriberOutcome::delivered()
		}
	}

	#[derive(Default)]
	struct FailFirstDispatcher {
		calls: Vec<JobId>,
	}

	impl JobDispatcher<()> for FailFirstDispatcher {
		fn owner_is_live(&self, _owner: RegistryOwnerId) -> bool {
			true
		}

		fn complete(
			&mut self,
			_context: &mut (),
			invocation: JobInvocation,
			_completion: &JobCompletion,
		) -> SubscriberOutcome {
			self.calls.push(invocation.job());
			if self.calls.len() == 1 {
				SubscriberOutcome::failed(BoundedStatus::fixed("first job callback failed"))
			} else {
				SubscriberOutcome::delivered()
			}
		}
	}

	fn receive_completion(
		runtime: &mut JobRuntime,
		requests: &mpsc::Receiver<EngineRequest>,
		id: JobId,
		owner_live: bool,
	) {
		let deadline = Instant::now() + Duration::from_secs(5);
		loop {
			let remaining = deadline.saturating_duration_since(Instant::now());
			let request = requests
				.recv_timeout(remaining)
				.expect("job completion request");
			if let EngineRequest::Internal(InternalEvent::JobCompleted(completed)) = request {
				assert_eq!(completed, id);
				runtime.handle_supervisor_report(id, owner_live);
				return;
			}
		}
	}

	fn deliver(runtime: &mut JobRuntime) -> RecordingDispatcher {
		runtime.capture_delivery_batch();
		let mut dispatcher = RecordingDispatcher::default();
		let mut steps = 0;
		let outcome = runtime.drain_deliveries(
			&mut (),
			&InvocationGate::default(),
			&mut dispatcher,
			&mut steps,
		);
		assert_eq!(
			outcome.result,
			super::super::event_subscribers::SubscriberResult::Delivered
		);
		dispatcher
	}

	#[test]
	fn admission_rejects_cross_owner_actions_without_consuming_an_id() {
		let (_, _, internal, _requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let mut invalid = spec(RegistryOwnerId::new(2));
		invalid.stdin = Some(vec![1, 2, 3]);
		assert_eq!(
			runtime.spawn(owner, true, invalid),
			Err(JobSpawnError::ActionOwnerMismatch)
		);
		assert_eq!(runtime.next_id, JobId::new(1));
		assert_eq!(runtime.charged_buffer_bytes(), 0);
	}

	#[test]
	fn reservation_is_admitted_without_starting_and_discard_releases_stdin() {
		let (_, _, internal, _requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let mut job = spec(owner);
		job.stdin = Some(vec![1, 2, 3]);
		let reservation = runtime.reserve_job(owner, true, job).expect("reserve job");
		assert_eq!(reservation.id(), JobId::new(1));
		assert_eq!(runtime.running, 0);
		assert!(runtime.queued.is_empty());
		assert_eq!(
			runtime.status(owner, reservation.id()),
			Ok(JobStatus::Queued)
		);
		assert_eq!(runtime.charged_buffer_bytes(), 3);
		runtime.discard_reservation(reservation);
		assert!(runtime.jobs.is_empty());
		assert_eq!(runtime.charged_buffer_bytes(), 0);
	}

	#[test]
	fn exhausted_global_buffer_budget_rejects_before_admission() {
		let (_, _, internal, _requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let held = runtime
			.budget
			.reserve(MAX_JOB_BUFFER_BYTES)
			.expect("hold global job budget");
		let mut job = spec(owner);
		job.stdin = Some(vec![1]);
		assert_eq!(
			runtime.spawn(owner, true, job),
			Err(JobSpawnError::BufferBudgetExhausted)
		);
		assert!(runtime.jobs.is_empty());
		assert_eq!(runtime.next_id, JobId::new(1));
		drop(held);
		assert_eq!(runtime.charged_buffer_bytes(), 0);
	}

	/// ASCII whose `os_len` is exactly `bytes`: one byte per char on Unix, two on
	/// Windows, so a cap stated in bytes is hit by fewer characters there.
	fn ascii_of_os_len(bytes: usize) -> String {
		"x".repeat(bytes / os_len_from_utf8("x"))
	}

	#[test]
	fn admission_caps_accept_exact_boundaries_and_reject_the_next_value() {
		let owner = RegistryOwnerId::new(1);
		let mut job = spec(owner);

		job.args = vec![OsString::new(); MAX_JOB_ARGS];
		assert_eq!(validate_spec(&job), Ok(()));
		job.args.push(OsString::new());
		assert_eq!(validate_spec(&job), Err(JobSpawnError::TooManyArguments));

		job.args = vec![OsString::from(ascii_of_os_len(MAX_JOB_ARG_BYTES))];
		assert_eq!(validate_spec(&job), Ok(()));
		job.args = vec![OsString::from(ascii_of_os_len(MAX_JOB_ARG_BYTES) + "x")];
		assert_eq!(validate_spec(&job), Err(JobSpawnError::ArgumentsTooLarge));

		job.args.clear();
		job.program = PathBuf::from(ascii_of_os_len(MAX_JOB_PROGRAM_BYTES));
		assert_eq!(validate_spec(&job), Ok(()));
		job.program = PathBuf::from(ascii_of_os_len(MAX_JOB_PROGRAM_BYTES) + "x");
		assert_eq!(validate_spec(&job), Err(JobSpawnError::ProgramTooLarge));

		job.program = PathBuf::from("/bin/true");
		job.cwd = Some(PathBuf::from(ascii_of_os_len(MAX_JOB_CWD_BYTES)));
		assert_eq!(validate_spec(&job), Ok(()));
		job.cwd = Some(PathBuf::from(ascii_of_os_len(MAX_JOB_CWD_BYTES) + "x"));
		assert_eq!(
			validate_spec(&job),
			Err(JobSpawnError::WorkingDirectoryTooLarge)
		);

		job.cwd = None;
		job.stdin = Some(vec![0; MAX_JOB_STDIN_BYTES]);
		assert_eq!(validate_spec(&job), Ok(()));
		job.stdin = Some(vec![0; MAX_JOB_STDIN_BYTES + 1]);
		assert_eq!(validate_spec(&job), Err(JobSpawnError::StdinTooLarge));

		let budget = JobBufferBudget::new();
		let exact = budget
			.reserve(MAX_JOB_BUFFER_BYTES)
			.expect("exact global buffer cap");
		assert!(budget.reserve(1).is_none());
		drop(exact);
		assert_eq!(budget.charged(), 0);
	}

	#[test]
	fn owner_and_session_caps_reject_before_consuming_an_id() {
		let (_, _, internal, _requests) = super::super::request::channel();
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		for index in 0..MAX_JOBS_PER_OWNER {
			let id = JobId::new((index + 1) as u64);
			runtime.jobs.insert(
				id,
				JobRecord {
					owner: RegistryOwnerId::new(1),
					lease: Some(ActionLease::new(RegistryOwnerId::new(1), FakeAction)),
					state: JobState::Deliverable(empty_completion(id, RegistryOwnerId::new(1))),
				},
			);
		}
		assert_eq!(
			runtime.spawn(RegistryOwnerId::new(1), true, spec(RegistryOwnerId::new(1))),
			Err(JobSpawnError::TooManyOwnerJobs)
		);
		assert_eq!(runtime.next_id, JobId::new(1));

		for index in MAX_JOBS_PER_OWNER..MAX_JOBS_TOTAL {
			let id = JobId::new((index + 1) as u64);
			let owner = RegistryOwnerId::new((index / MAX_JOBS_PER_OWNER + 1) as u64);
			runtime.jobs.insert(
				id,
				JobRecord {
					owner,
					lease: Some(ActionLease::new(owner, FakeAction)),
					state: JobState::Deliverable(empty_completion(id, owner)),
				},
			);
		}
		let fresh_owner = RegistryOwnerId::new(99);
		assert_eq!(
			runtime.spawn(fresh_owner, true, spec(fresh_owner)),
			Err(JobSpawnError::TooManySessionJobs)
		);
		assert_eq!(runtime.next_id, JobId::new(1));
	}

	#[test]
	fn status_is_typed_and_job_ids_are_never_reused() {
		let (_, _, internal, requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let mut sleeping = spec(owner);
		sleeping.program = PathBuf::from("/bin/sh");
		sleeping.args = vec![OsString::from("-c"), OsString::from("sleep 5")];
		let first = runtime.spawn(owner, true, sleeping).expect("first job");
		assert_eq!(runtime.status(owner, first), Ok(JobStatus::Running));
		assert_eq!(
			runtime.status(RegistryOwnerId::new(2), first),
			Err(UnknownJob)
		);
		assert_eq!(
			runtime.cancel(RegistryOwnerId::new(2), first),
			Err(JobCancelError::UnknownJob)
		);
		runtime.cancel(owner, first).expect("cancel first job");
		assert_eq!(
			runtime.status(owner, first),
			Ok(JobStatus::CancellationPending)
		);
		receive_completion(&mut runtime, &requests, first, true);
		assert_eq!(runtime.status(owner, first), Err(UnknownJob));

		let second = runtime.spawn(owner, true, spec(owner)).expect("second job");
		assert_eq!(second, first.next());
		receive_completion(&mut runtime, &requests, second, true);
		assert_eq!(runtime.status(owner, second), Ok(JobStatus::Completed));
		deliver(&mut runtime);
		assert_eq!(runtime.status(owner, second), Err(UnknownJob));

		runtime.next_id = JobId::new(u64::MAX);
		assert_eq!(
			runtime.spawn(owner, true, spec(owner)),
			Err(JobSpawnError::JobIdsExhausted)
		);
		assert_eq!(runtime.next_id, JobId::new(u64::MAX));
	}

	#[test]
	fn buffer_budget_charges_stdin_capacity_until_the_buffer_drops() {
		let (_, _, internal, _requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let mut bytes = Vec::with_capacity(4096);
		bytes.extend_from_slice(b"hello");
		let capacity = bytes.capacity();
		let mut job = spec(owner);
		job.program = PathBuf::from("/bin/cat");
		job.stdin = Some(bytes);
		let id = runtime.spawn(owner, true, job).expect("spawn admission");
		assert!(runtime.charged_buffer_bytes() >= capacity);
		runtime.cancel(owner, id).expect("cancel running job");
		runtime.shutdown();
		assert_eq!(runtime.charged_buffer_bytes(), 0);
	}

	#[test]
	fn capture_charges_its_exact_retained_capacity_before_allocation() {
		let budget = JobBufferBudget::new();
		let mut capture = JobCapture::default();
		capture.push(&budget, &[7; 1_003]);
		assert_eq!(capture.len(), 1_003);
		assert_eq!(budget.charged(), 1_003);
		drop(capture);
		assert_eq!(budget.charged(), 0);
	}

	#[test]
	fn bounded_job_errors_are_utf8_safe() {
		let error = BoundedJobError::new(JobErrorKind::Spawn, "é".repeat(MAX_JOB_ERROR_BYTES));
		assert!(error.message.len() <= MAX_JOB_ERROR_BYTES);
		assert!(error.message.is_char_boundary(error.message.len()));

		struct Verbose<'a>(&'a std::cell::Cell<usize>);
		impl fmt::Display for Verbose<'_> {
			fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
				for _ in 0..10_000 {
					self.0.set(self.0.get() + 1);
					fmt::Write::write_str(formatter, "diagnostic chunk")?;
				}
				Ok(())
			}
		}
		let writes = std::cell::Cell::new(0);
		let error = BoundedJobError::new(JobErrorKind::Io, Verbose(&writes));
		assert!(error.message.len() <= MAX_JOB_ERROR_BYTES);
		assert!(writes.get() < 10_000, "formatting must stop at the cap");
	}

	// Spawns /bin/sh fixtures; the portable supervisor has no Windows fixture yet.
	#[cfg(unix)]
	#[test]
	fn subprocess_round_trips_binary_stdin_and_releases_all_buffer_charge() {
		let (_, _, internal, requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let bytes = b"zero\0one\n\xff".to_vec();
		let mut job = spec(owner);
		job.program = PathBuf::from("/bin/cat");
		job.stdin = Some(bytes.clone());
		let id = runtime.spawn(owner, true, job).expect("spawn cat");
		receive_completion(&mut runtime, &requests, id, true);
		let dispatcher = deliver(&mut runtime);
		assert_eq!(dispatcher.completions.len(), 1);
		assert_eq!(
			dispatcher.completions[0].0,
			JobOutcome::Exited { status: 0 }
		);
		assert_eq!(dispatcher.completions[0].1, bytes);
		assert!(dispatcher.completions[0].2.is_empty());
		assert_eq!(runtime.charged_buffer_bytes(), 0);
	}

	// Spawns /bin/sh fixtures; the portable supervisor has no Windows fixture yet.
	#[cfg(unix)]
	#[test]
	fn completion_preserves_exit_status_and_separate_byte_streams() {
		let (_, _, internal, requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let mut job = spec(owner);
		job.program = PathBuf::from("/bin/sh");
		job.args = vec![
			OsString::from("-c"),
			OsString::from("cat; printf 'problem\\000bytes' >&2; exit 7"),
		];
		job.stdin = Some(b"input\0bytes".to_vec());
		let id = runtime
			.spawn(owner, true, job)
			.expect("spawn stream helper");
		receive_completion(&mut runtime, &requests, id, true);
		let dispatcher = deliver(&mut runtime);
		assert_eq!(
			dispatcher.completions[0].0,
			JobOutcome::Exited { status: 7 }
		);
		assert_eq!(dispatcher.completions[0].1, b"input\0bytes");
		assert_eq!(dispatcher.completions[0].2, b"problem\0bytes");
	}

	#[test]
	fn failed_exec_is_a_bounded_deliverable_completion() {
		let (_, _, internal, requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let mut job = spec(owner);
		job.program = PathBuf::from("/definitely/not/a/baryon-program");
		let id = runtime.spawn(owner, true, job).expect("admit failed exec");
		receive_completion(&mut runtime, &requests, id, true);
		let dispatcher = deliver(&mut runtime);
		let JobOutcome::SpawnFailed { error } = &dispatcher.completions[0].0 else {
			panic!("expected spawn failure")
		};
		assert_eq!(error.kind, JobErrorKind::Spawn);
		assert!(error.message.len() <= MAX_JOB_ERROR_BYTES);
	}

	#[test]
	fn failed_completion_callback_does_not_stop_its_sibling() {
		let (_, _, internal, requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let first = runtime.spawn(owner, true, spec(owner)).expect("first job");
		receive_completion(&mut runtime, &requests, first, true);
		let second = runtime.spawn(owner, true, spec(owner)).expect("second job");
		receive_completion(&mut runtime, &requests, second, true);
		runtime.capture_delivery_batch();
		let mut dispatcher = FailFirstDispatcher::default();
		let mut steps = 0;
		let outcome = runtime.drain_deliveries(
			&mut (),
			&InvocationGate::default(),
			&mut dispatcher,
			&mut steps,
		);
		assert_eq!(dispatcher.calls, [first, second]);
		assert_eq!(steps, 2);
		assert_eq!(
			outcome.foreground_status,
			Some(BoundedStatus::fixed("first job callback failed"))
		);
	}

	// Spawns /bin/sh fixtures; the portable supervisor has no Windows fixture yet.
	#[cfg(unix)]
	#[test]
	fn timeout_is_selected_when_its_report_reaches_the_engine_first() {
		let (_, _, internal, requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let mut job = spec(owner);
		job.program = PathBuf::from("/bin/sh");
		job.args = vec![OsString::from("-c"), OsString::from("sleep 5")];
		job.timeout = Duration::from_millis(20);
		let id = runtime
			.spawn(owner, true, job)
			.expect("spawn timeout helper");
		receive_completion(&mut runtime, &requests, id, true);
		assert_eq!(
			runtime.cancel(owner, id),
			Err(JobCancelError::AlreadyFinished)
		);
		let dispatcher = deliver(&mut runtime);
		assert_eq!(dispatcher.completions[0].0, JobOutcome::TimedOut);
	}

	#[test]
	fn accepted_cancel_suppresses_a_later_natural_or_timeout_report() {
		let (_, _, internal, requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let id = runtime
			.spawn(owner, true, spec(owner))
			.expect("spawn naturally exiting helper");
		runtime.cancel(owner, id).expect("accept cancellation");
		receive_completion(&mut runtime, &requests, id, true);
		assert!(deliver(&mut runtime).completions.is_empty());
	}

	// Spawns /bin/sh fixtures; the portable supervisor has no Windows fixture yet.
	#[cfg(unix)]
	#[test]
	fn running_slots_bound_children_and_queue_the_remainder_fifo() {
		let (_, _, internal, requests) = super::super::request::channel();
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let mut ids = Vec::new();
		for index in 0..MAX_RUNNING_JOBS + 2 {
			let owner = RegistryOwnerId::new((index / MAX_JOBS_PER_OWNER + 1) as u64);
			let mut job = spec(owner);
			job.program = PathBuf::from("/bin/sh");
			job.args = vec![OsString::from("-c"), OsString::from("sleep 5")];
			ids.push(
				runtime
					.spawn(owner, true, job)
					.expect("admit queued helper"),
			);
		}
		assert_eq!(runtime.running, MAX_RUNNING_JOBS);
		assert_eq!(
			runtime.queued,
			VecDeque::from([ids[MAX_RUNNING_JOBS], ids[MAX_RUNNING_JOBS + 1]])
		);

		// A queued cancellation never starts a process. Freeing a running slot
		// then starts the oldest surviving queued job.
		runtime
			.cancel(RegistryOwnerId::new(2), ids[MAX_RUNNING_JOBS + 1])
			.expect("cancel queued job");
		assert!(!runtime.jobs.contains_key(&ids[MAX_RUNNING_JOBS + 1]));
		runtime
			.cancel(RegistryOwnerId::new(1), ids[0])
			.expect("cancel running job");
		receive_completion(&mut runtime, &requests, ids[0], true);
		assert!(matches!(
			runtime
				.jobs
				.get(&ids[MAX_RUNNING_JOBS])
				.map(|record| &record.state),
			Some(JobState::Running(_))
		));
		assert!(runtime.queued.is_empty());
		runtime.shutdown();
	}

	#[test]
	fn global_capture_pressure_truncates_without_exceeding_the_budget() {
		let budget = JobBufferBudget::new();
		let held = budget
			.reserve(MAX_JOB_BUFFER_BYTES - 8)
			.expect("reserve almost all budget");
		let mut capture = JobCapture::default();
		capture.push(&budget, &[7; 16]);
		assert!(capture.truncated);
		assert_eq!(capture.len(), 0);
		assert_eq!(budget.charged(), MAX_JOB_BUFFER_BYTES - 8);
		drop(held);
		assert_eq!(budget.charged(), 0);
	}

	#[test]
	fn concurrent_buffer_reservations_never_exceed_the_session_cap() {
		let budget = JobBufferBudget::new();
		let barrier = Arc::new(std::sync::Barrier::new(3));
		let (tx, rx) = mpsc::channel();
		let mut workers = Vec::new();
		for _ in 0..2 {
			let budget = budget.clone();
			let barrier = barrier.clone();
			let tx = tx.clone();
			workers.push(thread::spawn(move || {
				barrier.wait();
				let reservation = budget.reserve(40 * 1024 * 1024);
				tx.send(reservation.is_some()).expect("reservation result");
				barrier.wait();
				drop(reservation);
			}));
		}
		barrier.wait();
		let accepted = usize::from(rx.recv().unwrap()) + usize::from(rx.recv().unwrap());
		assert_eq!(accepted, 1);
		assert_eq!(budget.charged(), 40 * 1024 * 1024);
		barrier.wait();
		for worker in workers {
			worker.join().unwrap();
		}
		assert_eq!(budget.charged(), 0);
	}

	#[test]
	fn shutdown_is_bounded_even_with_term_ignoring_children() {
		let (_, _, internal, _requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let mut job = spec(owner);
		job.program = PathBuf::from("/bin/sh");
		job.args = vec![
			OsString::from("-c"),
			OsString::from("trap '' TERM; (trap '' TERM; sleep 5) & wait"),
		];
		runtime
			.spawn(owner, true, job)
			.expect("spawn stubborn tree");
		let started = Instant::now();
		runtime.shutdown();
		assert!(
			started.elapsed() < Duration::from_millis(100),
			"job shutdown took {:?}",
			started.elapsed()
		);
	}

	#[cfg(target_os = "linux")]
	#[test]
	fn shutdown_kills_the_supervisors_process_group() {
		let (_, _, internal, _requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let pidfile = std::env::temp_dir().join(format!(
			"baryon-job-grandchild-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.expect("clock after epoch")
				.as_nanos()
		));
		let mut job = spec(owner);
		job.program = PathBuf::from("/bin/sh");
		job.args = vec![
			OsString::from("-c"),
			OsString::from(format!(
				"trap '' TERM; (trap '' TERM; while :; do sleep 1; done) & echo $! > '{}'; wait",
				pidfile.display()
			)),
		];
		runtime.spawn(owner, true, job).expect("spawn process tree");
		let ready_by = Instant::now() + Duration::from_secs(2);
		while !pidfile.exists() && Instant::now() < ready_by {
			thread::sleep(Duration::from_millis(2));
		}
		let pid: i32 = std::fs::read_to_string(&pidfile)
			.expect("grandchild pidfile")
			.trim()
			.parse()
			.expect("grandchild pid");
		runtime.shutdown();

		let gone_by = Instant::now() + Duration::from_millis(500);
		loop {
			let result = unsafe { libc::kill(pid, 0) };
			if result == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
				break;
			}
			assert!(
				Instant::now() < gone_by,
				"grandchild {pid} survived process-group shutdown"
			);
			thread::sleep(Duration::from_millis(2));
		}
		let _ = std::fs::remove_file(pidfile);
	}

	#[cfg(target_os = "linux")]
	#[test]
	fn timeout_escalates_and_reaps_the_whole_process_group() {
		let (_, _, internal, requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let pidfile = std::env::temp_dir().join(format!(
			"baryon-job-timeout-tree-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.expect("clock after epoch")
				.as_nanos()
		));
		let mut job = spec(owner);
		job.program = PathBuf::from("/bin/sh");
		job.args = vec![
			OsString::from("-c"),
			OsString::from(format!(
				"trap '' TERM; (trap '' TERM; while :; do sleep 1; done) & echo \"$$ $!\" > '{}'; wait",
				pidfile.display()
			)),
		];
		job.timeout = Duration::from_millis(20);
		let started = Instant::now();
		let id = runtime.spawn(owner, true, job).expect("spawn timeout tree");
		let ready_by = Instant::now() + Duration::from_secs(2);
		while !pidfile.exists() && Instant::now() < ready_by {
			thread::sleep(Duration::from_millis(2));
		}
		let pids: Vec<i32> = std::fs::read_to_string(&pidfile)
			.expect("process-tree pidfile")
			.split_whitespace()
			.map(|pid| pid.parse().expect("process-tree pid"))
			.collect();
		assert_eq!(pids.len(), 2);

		receive_completion(&mut runtime, &requests, id, true);
		assert!(started.elapsed() >= JOB_KILL_GRACE);
		let dispatcher = deliver(&mut runtime);
		assert_eq!(dispatcher.completions[0].0, JobOutcome::TimedOut);
		for pid in pids {
			let gone_by = Instant::now() + Duration::from_millis(500);
			loop {
				let result = unsafe { libc::kill(pid, 0) };
				if result == -1
					&& std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
				{
					break;
				}
				assert!(
					Instant::now() < gone_by,
					"process-group member {pid} survived timeout escalation"
				);
				thread::sleep(Duration::from_millis(2));
			}
		}
		let _ = std::fs::remove_file(pidfile);
	}

	// Spawns /bin/sh fixtures; the portable supervisor has no Windows fixture yet.
	#[cfg(unix)]
	#[test]
	fn output_before_stdin_does_not_deadlock_the_supervisor() {
		let (_, _, internal, requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let stdin = vec![b'x'; 1024 * 1024];
		let mut job = spec(owner);
		job.program = PathBuf::from("/bin/sh");
		job.args = vec![
			OsString::from("-c"),
			OsString::from("head -c 2097152 /dev/zero; cat"),
		];
		job.stdin = Some(stdin.clone());
		let id = runtime
			.spawn(owner, true, job)
			.expect("spawn pipe stress helper");
		receive_completion(&mut runtime, &requests, id, true);
		let dispatcher = deliver(&mut runtime);
		let stdout = &dispatcher.completions[0].1;
		assert_eq!(stdout.len(), 2 * 1024 * 1024 + stdin.len());
		assert!(stdout[..2 * 1024 * 1024].iter().all(|byte| *byte == 0));
		assert_eq!(&stdout[2 * 1024 * 1024..], stdin);
	}

	// Spawns /bin/sh fixtures; the portable supervisor has no Windows fixture yet.
	#[cfg(unix)]
	#[test]
	fn output_over_the_stream_cap_is_drained_but_marked_truncated() {
		let (_, _, internal, requests) = super::super::request::channel();
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let mut job = spec(owner);
		job.program = PathBuf::from("/bin/sh");
		job.args = vec![
			OsString::from("-c"),
			OsString::from("head -c 9437184 /dev/zero"),
		];
		let id = runtime
			.spawn(owner, true, job)
			.expect("spawn truncation helper");
		receive_completion(&mut runtime, &requests, id, true);
		let record = runtime.jobs.get(&id).expect("deliverable job");
		let JobState::Deliverable(completion) = &record.state else {
			panic!("job must be deliverable")
		};
		assert_eq!(completion.stdout.len(), MAX_JOB_CAPTURE_BYTES);
		assert!(completion.stdout.truncated);
		let dispatcher = deliver(&mut runtime);
		assert_eq!(dispatcher.completions[0].1.len(), MAX_JOB_CAPTURE_BYTES);
	}

	#[test]
	fn full_ordered_request_stream_retries_and_shutdown_escapes() {
		let (_commands, _input, internal, requests) = super::super::request::channel();
		loop {
			match internal.try_send(InternalEvent::Refresh) {
				Ok(()) => {}
				Err(mpsc::TrySendError::Full(_)) => break,
				Err(mpsc::TrySendError::Disconnected(_)) => panic!("request stream disconnected"),
			}
		}
		let owner = RegistryOwnerId::new(1);
		let mut runtime = JobRuntime::new();
		runtime.attach_internal_sender(internal);
		let id = runtime
			.spawn(owner, true, spec(owner))
			.expect("spawn on full stream");
		thread::sleep(Duration::from_millis(20));
		let mut completed = false;
		while let Ok(request) = requests.try_recv() {
			if matches!(request, EngineRequest::Internal(InternalEvent::JobCompleted(job)) if job == id)
			{
				runtime.handle_supervisor_report(id, true);
				completed = true;
			}
		}
		if !completed {
			receive_completion(&mut runtime, &requests, id, true);
		}

		loop {
			match runtime
				.internal
				.as_ref()
				.expect("internal sender")
				.try_send(InternalEvent::Refresh)
			{
				Ok(()) => {}
				Err(mpsc::TrySendError::Full(_)) => break,
				Err(mpsc::TrySendError::Disconnected(_)) => panic!("request stream disconnected"),
			}
		}
		let second = runtime.spawn(owner, true, spec(owner)).expect("second job");
		let started = Instant::now();
		runtime.shutdown();
		assert!(started.elapsed() < Duration::from_millis(100));
		assert!(!runtime.jobs.contains_key(&second));
	}
}
