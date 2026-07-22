use super::facade::{DeferredRuneJob, EditorHost, HostByteReservation, lock_services};
use crate::core::{JobId, RegistryOwnerId};
use crate::engine::jobs::{
	JobCompletion, JobOutcome, JobSpawnError, JobSpec, JobStatus, os_len_from_utf8,
	validate_spec_limits,
};
use rune::alloc::fmt::TryWrite;
use rune::alloc::{String as RuneString, Vec as RuneVec};
use rune::runtime::{Bytes, Formatter, Function, Protocol, VmResult};
use rune::{Any, Module};
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Any, Debug)]
#[rune(item = ::baryon, name = JobSpec)]
pub(crate) struct RuneJobSpec {
	program: RuneString,
	args: RuneVec<RuneString>,
	cwd: Option<RuneString>,
	stdin: Option<Bytes>,
	timeout_ms: u64,
}

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon, name = Job)]
pub(crate) struct RuneJob {
	owner: RegistryOwnerId,
	id: JobId,
}

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon, name = JobStatus)]
pub(crate) enum RuneJobStatus {
	#[rune(constructor)]
	Queued,
	#[rune(constructor)]
	Running,
	#[rune(constructor)]
	CancellationPending,
}

#[derive(Any, Debug)]
#[rune(item = ::baryon, name = JobError)]
pub(crate) struct RuneJobError {
	#[rune(get, copy)]
	kind: RuneJobErrorKind,
	#[rune(get)]
	message: RuneString,
}

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon, name = JobErrorKind)]
pub(crate) enum RuneJobErrorKind {
	#[rune(constructor)]
	MalformedSpec,
	#[rune(constructor)]
	TooManyOwnerJobs,
	#[rune(constructor)]
	TooManySessionJobs,
	#[rune(constructor)]
	BufferBudgetExhausted,
	#[rune(constructor)]
	RuntimeUnavailable,
	#[rune(constructor)]
	StaleHandle,
	#[rune(constructor)]
	AlreadyFinished,
}

#[derive(Any, Debug)]
#[rune(item = ::baryon, name = JobResult)]
pub(crate) struct RuneJobResult {
	outcome: RuneJobOutcome,
	runtime_ms: u64,
	stdout: Arc<crate::engine::jobs::JobCapture>,
	stderr: Arc<crate::engine::jobs::JobCapture>,
	stdout_truncated: bool,
	stderr_truncated: bool,
}

#[derive(Any, Debug, Clone, Copy)]
#[rune(item = ::baryon, name = JobOutcome)]
pub(crate) enum RuneJobOutcome {
	#[rune(constructor)]
	Exited(#[rune(get, copy)] i64),
	#[rune(constructor)]
	Signaled,
	#[rune(constructor)]
	TimedOut,
	#[rune(constructor)]
	SpawnFailed,
}

impl RuneJob {
	pub(crate) fn from_invocation(invocation: &crate::engine::jobs::JobInvocation) -> Self {
		Self {
			owner: invocation.owner(),
			id: invocation.job(),
		}
	}
}
impl RuneJobResult {
	pub(crate) fn from_completion(value: &JobCompletion) -> Self {
		let outcome = match &value.outcome {
			JobOutcome::Exited { status } => RuneJobOutcome::Exited(i64::from(*status)),
			JobOutcome::Signaled { .. } => RuneJobOutcome::Signaled,
			JobOutcome::TimedOut => RuneJobOutcome::TimedOut,
			JobOutcome::SpawnFailed { .. } => RuneJobOutcome::SpawnFailed,
			JobOutcome::Cancelled => {
				// JobRuntime suppresses cancelled records before delivery. Keep a
				// defensive value for release builds, but flag an invariant break
				// during development instead of silently treating it as a spawn error.
				debug_assert!(false, "cancelled job completion reached the Rune host");
				RuneJobOutcome::SpawnFailed
			}
		};
		Self {
			outcome,
			runtime_ms: value.runtime.as_millis().min(u128::from(u64::MAX)) as u64,
			stdout: Arc::clone(&value.stdout),
			stderr: Arc::clone(&value.stderr),
			stdout_truncated: value.stdout.truncated,
			stderr_truncated: value.stderr.truncated,
		}
	}
}

impl RuneJobSpec {
	#[rune::function(path = Self::new)]
	fn new(program: RuneString, args: RuneVec<RuneString>) -> VmResult<Result<Self, RuneJobError>> {
		VmResult::Ok(Ok(Self {
			program,
			args,
			cwd: None,
			stdin: None,
			timeout_ms: 120_000,
		}))
	}
	#[rune::function(path = Self::with_cwd)]
	fn with_cwd(mut self, cwd: RuneString) -> VmResult<Result<Self, RuneJobError>> {
		self.cwd = Some(cwd);
		VmResult::Ok(Ok(self))
	}
	#[rune::function(path = Self::with_stdin)]
	fn with_stdin(mut self, stdin: Bytes) -> VmResult<Result<Self, RuneJobError>> {
		self.stdin = Some(stdin);
		VmResult::Ok(Ok(self))
	}
	#[rune::function(path = Self::with_timeout_ms)]
	fn with_timeout_ms(mut self, timeout_ms: u64) -> VmResult<Result<Self, RuneJobError>> {
		self.timeout_ms = timeout_ms;
		VmResult::Ok(Ok(self))
	}
}

fn error(kind: RuneJobErrorKind, message: impl ToString) -> RuneJobError {
	RuneJobError {
		kind,
		message: RuneString::try_from(message.to_string()).unwrap_or_default(),
	}
}

fn malformed(message: impl ToString) -> RuneJobError {
	error(RuneJobErrorKind::MalformedSpec, message)
}

fn spawn_error(spawn_error: JobSpawnError) -> RuneJobError {
	let kind = match spawn_error {
		JobSpawnError::TooManyOwnerJobs => RuneJobErrorKind::TooManyOwnerJobs,
		JobSpawnError::TooManySessionJobs => RuneJobErrorKind::TooManySessionJobs,
		JobSpawnError::BufferBudgetExhausted => RuneJobErrorKind::BufferBudgetExhausted,
		JobSpawnError::RuntimeUnavailable | JobSpawnError::RetiredOwner(_) => {
			RuneJobErrorKind::RuntimeUnavailable
		}
		JobSpawnError::ActionOwnerMismatch
		| JobSpawnError::JobIdsExhausted
		| JobSpawnError::TooManyArguments
		| JobSpawnError::ArgumentsTooLarge
		| JobSpawnError::ProgramTooLarge
		| JobSpawnError::WorkingDirectoryTooLarge
		| JobSpawnError::StdinTooLarge => RuneJobErrorKind::MalformedSpec,
	};
	error(kind, spawn_error)
}
fn charge(editor: &EditorHost) -> Result<(), &'static str> {
	lock_services(&editor.services).budget.charge()
}

fn preflight_spec(spec: &RuneJobSpec) -> Result<usize, RuneJobError> {
	let argument_bytes = spec.args.iter().try_fold(0usize, |total, argument| {
		total.checked_add(os_len_from_utf8(argument.as_str()))
	});
	let program_bytes = os_len_from_utf8(spec.program.as_str());
	let cwd_bytes = spec.cwd.as_ref().map(|cwd| os_len_from_utf8(cwd.as_str()));
	let stdin_bytes = spec.stdin.as_ref().map(Bytes::len);
	validate_spec_limits(
		spec.args.len(),
		argument_bytes,
		program_bytes,
		cwd_bytes,
		stdin_bytes,
	)
	.map_err(spawn_error)?;
	program_bytes
		.checked_add(argument_bytes.expect("checked above"))
		.and_then(|bytes| bytes.checked_add(cwd_bytes.unwrap_or_default()))
		.and_then(|bytes| bytes.checked_add(stdin_bytes.unwrap_or_default()))
		.ok_or_else(|| malformed("Rune deferred-job metadata overflowed"))
}

fn spawn(
	editor: &mut EditorHost,
	spec: RuneJobSpec,
	completion: Function,
) -> VmResult<Result<RuneJob, RuneJobError>> {
	if let Err(e) = charge(editor) {
		return VmResult::panic(e);
	}
	let Some(owner) = editor.active_callback_owner() else {
		return VmResult::Ok(Err(error(
			RuneJobErrorKind::RuntimeUnavailable,
			"Rune jobs are only valid during a callback",
		)));
	};
	let metadata = match preflight_spec(&spec) {
		Ok(metadata) => metadata,
		Err(error) => return VmResult::Ok(Err(error)),
	};
	let args = spec
		.args
		.iter()
		.map(|v| OsString::from(v.as_str()))
		.collect();
	let engine_spec = JobSpec {
		program: PathBuf::from(spec.program.as_str()),
		args,
		cwd: spec.cwd.as_ref().map(|v| PathBuf::from(v.as_str())),
		stdin: spec.stdin.as_ref().map(|v| v.as_slice().to_vec()),
		timeout: Duration::from_millis(spec.timeout_ms),
		completion: None,
	};
	let reservation = match editor.session.jobs.reserve_job(
		owner,
		editor.session.runtime_registries.owner_is_live(owner),
		engine_spec,
	) {
		Ok(v) => v,
		Err(e) => return VmResult::Ok(Err(spawn_error(e))),
	};
	let job = RuneJob {
		owner,
		id: reservation.id(),
	};
	if let Err(e) = editor.defer_job(
		DeferredRuneJob {
			reservation,
			completion,
		},
		metadata,
	) {
		editor.session.jobs.discard_reservation(reservation);
		return VmResult::Ok(Err(malformed(e)));
	}
	VmResult::Ok(Ok(job))
}

fn cancel(job: &RuneJob, editor: &mut EditorHost) -> VmResult<Result<(), RuneJobError>> {
	if let Err(e) = charge(editor) {
		return VmResult::panic(e);
	}
	if editor.active_callback_owner() != Some(job.owner) {
		return VmResult::Ok(Err(error(
			RuneJobErrorKind::StaleHandle,
			"stale or cross-owner job handle",
		)));
	}
	VmResult::Ok(
		editor
			.session
			.jobs
			.cancel(job.owner, job.id)
			.map_err(|_| error(RuneJobErrorKind::AlreadyFinished, "unknown or finished job")),
	)
}
fn status(job: &RuneJob, editor: &mut EditorHost) -> VmResult<Result<RuneJobStatus, RuneJobError>> {
	if let Err(e) = charge(editor) {
		return VmResult::panic(e);
	}
	if editor.active_callback_owner() != Some(job.owner) {
		return VmResult::Ok(Err(error(
			RuneJobErrorKind::StaleHandle,
			"stale or cross-owner job handle",
		)));
	}
	let status = match editor.session.jobs.status(job.owner, job.id) {
		Ok(JobStatus::Queued) => Ok(RuneJobStatus::Queued),
		Ok(JobStatus::Running) => Ok(RuneJobStatus::Running),
		Ok(JobStatus::CancellationPending) => Ok(RuneJobStatus::CancellationPending),
		Ok(JobStatus::Completed) => Err(error(
			RuneJobErrorKind::AlreadyFinished,
			"completed job is no longer queryable",
		)),
		Err(_) => Err(error(RuneJobErrorKind::StaleHandle, "unknown job")),
	};
	VmResult::Ok(status)
}

fn result_outcome(value: &RuneJobResult) -> RuneJobOutcome {
	value.outcome
}
fn result_runtime_ms(value: &RuneJobResult) -> u64 {
	value.runtime_ms
}
fn result_stdout(
	value: &RuneJobResult,
	editor: &mut EditorHost,
) -> VmResult<Result<Bytes, RuneJobError>> {
	if let Err(e) = charge(editor) {
		return VmResult::panic(e);
	}
	let reservation = match HostByteReservation::read(&editor.services, value.stdout.len()) {
		Ok(reservation) => reservation,
		Err(message) => return VmResult::panic(message),
	};
	match bytes_from_capture(&value.stdout) {
		Ok(bytes) => match reservation.complete(bytes.len()) {
			Ok(()) => VmResult::Ok(Ok(bytes)),
			Err(message) => VmResult::panic(message),
		},
		Err(_) => VmResult::Ok(Err(malformed("Rune allocation failed"))),
	}
}
fn result_stderr(
	value: &RuneJobResult,
	editor: &mut EditorHost,
) -> VmResult<Result<Bytes, RuneJobError>> {
	if let Err(e) = charge(editor) {
		return VmResult::panic(e);
	}
	let reservation = match HostByteReservation::read(&editor.services, value.stderr.len()) {
		Ok(reservation) => reservation,
		Err(message) => return VmResult::panic(message),
	};
	match bytes_from_capture(&value.stderr) {
		Ok(bytes) => match reservation.complete(bytes.len()) {
			Ok(()) => VmResult::Ok(Ok(bytes)),
			Err(message) => VmResult::panic(message),
		},
		Err(_) => VmResult::Ok(Err(malformed("Rune allocation failed"))),
	}
}
fn result_stdout_truncated(value: &RuneJobResult) -> bool {
	value.stdout_truncated
}
fn result_stderr_truncated(value: &RuneJobResult) -> bool {
	value.stderr_truncated
}

fn bytes_from_capture(
	capture: &crate::engine::jobs::JobCapture,
) -> Result<Bytes, rune::alloc::Error> {
	let mut bytes = RuneVec::try_with_capacity(capture.len())?;
	for chunk in capture.chunks() {
		bytes.try_extend_from_slice(chunk)?;
	}
	Ok(Bytes::from(bytes))
}

fn clone_job(job: &RuneJob) -> RuneJob {
	*job
}

fn eq_job(left: &RuneJob, right: &RuneJob) -> bool {
	left == right
}

fn debug_job(job: &RuneJob, formatter: &mut Formatter) -> VmResult<()> {
	rune::vm_write!(formatter, "Job({}:{})", job.owner, job.id.get())
}

pub(crate) fn install(module: &mut Module) -> Result<(), rune::ContextError> {
	module.ty::<RuneJobSpec>()?;
	module.ty::<RuneJob>()?;
	module.ty::<RuneJobStatus>()?;
	module.ty::<RuneJobError>()?;
	module.ty::<RuneJobResult>()?;
	module.ty::<RuneJobOutcome>()?;
	module.function_meta(RuneJobSpec::new)?;
	module.function_meta(RuneJobSpec::with_cwd)?;
	module.function_meta(RuneJobSpec::with_stdin)?;
	module.function_meta(RuneJobSpec::with_timeout_ms)?;
	module.function("spawn", spawn).build()?;
	module.associated_function("cancel", cancel)?;
	module.associated_function("status", status)?;
	module.associated_function("outcome", result_outcome)?;
	module.associated_function("runtime_ms", result_runtime_ms)?;
	module.associated_function("stdout", result_stdout)?;
	module.associated_function("stderr", result_stderr)?;
	module.associated_function("stdout_truncated", result_stdout_truncated)?;
	module.associated_function("stderr_truncated", result_stderr_truncated)?;
	module.associated_function(&Protocol::CLONE, clone_job)?;
	module.associated_function(&Protocol::PARTIAL_EQ, eq_job)?;
	module.associated_function(&Protocol::DEBUG_FMT, debug_job)?;
	module.implement_trait::<RuneJob>(rune::item!(::std::clone::Clone))?;
	module.implement_trait::<RuneJob>(rune::item!(::std::cmp::PartialEq))?;
	Ok(())
}
