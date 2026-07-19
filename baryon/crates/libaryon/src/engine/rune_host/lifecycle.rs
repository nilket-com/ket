use super::autocmd::{RuneAutocmdBaseline, RuneAutocmdEvent, RuneAutocmdRef, event_matches_buffer};
use super::diagnostics::{render_diagnostics, render_error};
use super::facade::{
	DEFAULT_HOST_CALL_QUOTA, DeferredRuneJob, DeferredRuneLifecycle, EditorHost, HostEffects,
	SharedHostServices, lock_services,
};
use super::message::MAX_MESSAGE_LINE_BYTES;
use super::registration::{
	RuneCallbackRef, SourceStaging, register_command, register_mapping, register_option,
};
use super::source_worker::{SourceCompileResult, SourceCompiler};
use super::{autocmd, buffer_api, config_api, job_api};
use crate::core::{RegistryOwnerId, SourceId, SubscriptionId};
use crate::engine::event_bus::EngineEvent;
use crate::engine::event_subscribers::{
	BaselineBuilder, BaselineContributor, BaselineDomain, BaselineError, BoundedStatus,
	SubscriberBaseline, SubscriberDispatcher, SubscriberOutcome, SubscriberResult,
};
use crate::engine::jobs::JobCompletion;
use crate::engine::request::InternalSender;
use crate::engine::runtime_registry::ActionLease;
use crate::engine::runtime_registry::{
	ActionError, DynamicActionExecutor, DynamicActionOutcome, DynamicInvocation,
	InvocationSnapshot, OptionInvocation, OptionValue, SubscriberInvocation,
};
use rune::alloc::limit;
use rune::runtime::budget;
use rune::runtime::{Function, InstAddress, Memory, Output, RuntimeContext, VmResult};
use rune::{Any, Value};
use rune::{Context, Diagnostics, Module, Source, Sources, Vm};
use std::collections::{BTreeMap, VecDeque};
use std::fmt::{self, Write};
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) const MAX_SOURCE_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const COMPILE_ALLOCATION_CAP: usize = 256 * 1024 * 1024;
pub(crate) const STARTUP_ALLOCATION_CAP: usize = 256 * 1024 * 1024;
pub(crate) const STARTUP_INSTRUCTION_BUDGET: usize = 50_000_000;
pub(crate) const CALLBACK_ALLOCATION_CAP: usize = 64 * 1024 * 1024;
pub(crate) const CALLBACK_INSTRUCTION_BUDGET: usize = 10_000_000;
pub(crate) const SESSION_RETAINED_CAP: usize = 512 * 1024 * 1024;
pub(crate) const REPLACEMENT_HEADROOM: usize = 256 * 1024 * 1024;
pub(crate) const MAX_RETAINED_PER_GENERATION: usize = 256 * 1024 * 1024;
pub(crate) const MAX_STATUS_BYTES: usize = 4 * 1024;
const MAX_HOST_OUTPUT_CALL_BYTES: usize = 64 * 1024;
const MAX_DBG_ARGUMENTS: usize = 64;
pub(crate) const MAX_SOURCE_SLOTS: usize = 4_096;
pub(crate) const MAX_SOURCE_KEY_BYTES: usize = 32 * 1024;
pub(crate) const MAX_SOURCE_KEY_TOTAL_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_AUTOCMD_CONSECUTIVE_FAILURES: u8 = 8;

#[derive(Debug, Clone)]
pub(crate) struct RuneHostConfig {
	init_script: PathBuf,
}

impl RuneHostConfig {
	pub(crate) fn new(init_script: PathBuf) -> Self {
		Self { init_script }
	}
}

#[derive(Debug, Default)]
pub(crate) struct StartupScriptReport {
	pub(crate) error: Option<String>,
	pub(crate) output: Option<String>,
	pub(crate) needs_render: bool,
}

impl StartupScriptReport {
	pub(crate) fn foreground_status(&self) -> Option<String> {
		self.error.clone().or_else(|| self.output.clone())
	}
}

pub(crate) struct SourceCompileReport {
	pub(crate) status: Option<String>,
	pub(crate) needs_render: bool,
}

pub(crate) struct RuneHost {
	config: RuneHostConfig,
	configured_init_path: PathBuf,
	context: Option<Context>,
	runtime: Arc<RuntimeContext>,
	services: SharedHostServices,
	generations: BTreeMap<RegistryOwnerId, Generation>,
	sources: BTreeMap<SourceKey, SourceSlot>,
	next_source_id: SourceId,
	next_source_request_id: SourceRequestId,
	retained: RetainedRuneBudget,
	internal: InternalSender,
	compiler: Option<SourceCompiler>,
	pending_compile: Option<PendingCompile>,
	queued_order: VecDeque<SourceKey>,
	queued_sources: BTreeMap<SourceKey, QueuedSource>,
	source_key_bytes: usize,
	autocmd_failures: BTreeMap<RegistryOwnerId, BTreeMap<SubscriptionId, u8>>,
}

struct PendingCompile {
	key: SourceKey,
	request: SourceRequestId,
	reservation: usize,
}

struct QueuedSource {
	request: SourceRequestId,
	path: PathBuf,
}

struct Generation {
	unit: Arc<rune::Unit>,
	callbacks: Vec<Function>,
	retained_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum SourceKey {
	Init,
	Path(PathBuf),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct SourceRequestId(u64);

impl SourceRequestId {
	const FIRST: Self = Self(1);

	fn next(self) -> Option<Self> {
		self.0.checked_add(1).map(Self)
	}
}

struct SourceSlot {
	id: SourceId,
	current_owner: Option<RegistryOwnerId>,
	latest_request: SourceRequestId,
}

struct RetainedRuneBudget {
	charged: usize,
	reserved: usize,
}

impl RetainedRuneBudget {
	fn new() -> Self {
		Self {
			charged: 0,
			reserved: 0,
		}
	}

	fn reserve_compile(&mut self) -> Result<usize, String> {
		if self.reserved != 0 {
			return Err("A Rune compilation is already pending".to_string());
		}
		let peak = SESSION_RETAINED_CAP
			.checked_add(REPLACEMENT_HEADROOM)
			.and_then(|cap| cap.checked_sub(self.charged))
			.ok_or_else(|| "Rune retained-memory accounting overflowed".to_string())?;
		let reservation = COMPILE_ALLOCATION_CAP.min(peak);
		if reservation == 0 {
			return Err("Rune retained-memory limit is exhausted".to_string());
		}
		self.reserved = reservation;
		Ok(reservation)
	}

	fn retain_compiled(&mut self, reservation: usize, retained: usize) -> Result<(), String> {
		if self.reserved != reservation || retained > reservation {
			return Err("Rune compile reservation accounting mismatch".to_string());
		}
		self.reserved = retained;
		Ok(())
	}

	fn release_reservation(&mut self) {
		self.reserved = 0;
	}

	fn evaluation_allowance(&self, unit_bytes: usize) -> Result<usize, String> {
		if self.reserved != unit_bytes {
			return Err("Rune candidate Unit is not reserved".to_string());
		}
		let peak_free = SESSION_RETAINED_CAP
			.checked_add(REPLACEMENT_HEADROOM)
			.and_then(|cap| cap.checked_sub(self.charged))
			.and_then(|free| free.checked_sub(unit_bytes))
			.ok_or_else(|| "Rune replacement headroom is exhausted".to_string())?;
		Ok(STARTUP_ALLOCATION_CAP.min(peak_free))
	}

	fn admit_generation(
		&self,
		unit_bytes: usize,
		evaluation_bytes: usize,
		retired_bytes: usize,
	) -> Result<usize, String> {
		let candidate = unit_bytes
			.checked_add(evaluation_bytes)
			.ok_or_else(|| "Rune generation accounting overflowed".to_string())?;
		if candidate > MAX_RETAINED_PER_GENERATION {
			return Err("Rune generation retained-memory limit is exceeded".to_string());
		}
		let peak = self
			.charged
			.checked_add(candidate)
			.ok_or_else(|| "Rune peak accounting overflowed".to_string())?;
		if peak > SESSION_RETAINED_CAP + REPLACEMENT_HEADROOM {
			return Err("Rune replacement headroom is exceeded".to_string());
		}
		let steady = self
			.charged
			.checked_sub(retired_bytes)
			.and_then(|charged| charged.checked_add(candidate))
			.ok_or_else(|| "Rune steady-state accounting overflowed".to_string())?;
		if steady > SESSION_RETAINED_CAP {
			return Err("Rune session retained-memory limit is exceeded".to_string());
		}
		Ok(candidate)
	}

	fn commit_generation(&mut self, candidate: usize, retired_bytes: usize) {
		self.charged = self
			.charged
			.checked_sub(retired_bytes)
			.and_then(|charged| charged.checked_add(candidate))
			.expect("preflighted Rune generation accounting must commit");
		self.reserved = 0;
	}

	fn callback_allowance(&self, generation_bytes: usize) -> usize {
		let steady_free = SESSION_RETAINED_CAP.saturating_sub(self.charged);
		let peak_free = (SESSION_RETAINED_CAP + REPLACEMENT_HEADROOM)
			.saturating_sub(self.charged)
			.saturating_sub(self.reserved);
		let generation_free = MAX_RETAINED_PER_GENERATION.saturating_sub(generation_bytes);
		CALLBACK_ALLOCATION_CAP
			.min(steady_free)
			.min(peak_free)
			.min(generation_free)
	}

	fn apply_callback_delta(
		&mut self,
		generation: &mut Generation,
		delta: i128,
	) -> Result<(), String> {
		let next_generation = apply_signed_bytes(generation.retained_bytes, delta)?;
		let next_charged = apply_signed_bytes(self.charged, delta)?;
		generation.retained_bytes = next_generation;
		self.charged = next_charged;
		Ok(())
	}
}

fn apply_signed_bytes(value: usize, delta: i128) -> Result<usize, String> {
	if delta >= 0 {
		let growth = usize::try_from(delta)
			.map_err(|_| "Rune retained-memory growth overflowed".to_string())?;
		value
			.checked_add(growth)
			.ok_or_else(|| "Rune retained-memory accounting overflowed".to_string())
	} else {
		let released = usize::try_from(-delta)
			.map_err(|_| "Rune retained-memory release overflowed".to_string())?;
		value
			.checked_sub(released)
			.ok_or_else(|| "Rune released uncharged memory".to_string())
	}
}

#[derive(Any)]
#[rune(item = ::baryon)]
#[allow(dead_code)]
enum OptionCall {
	Query,
	SetBool(bool),
	SetInteger(i64),
	SetString(String),
}

/// Why a VM entry was rejected before the callback ran. The generation is
/// left exactly as it was found; each entry path maps these to its own
/// error channel and message text.
enum VmEntryRejection {
	RetiredOwner,
	InvalidCallback,
	AllowanceExhausted,
}

/// Everything a completed VM entry produced. Error precedence differs by
/// entry path, so the components are returned unfolded and each path
/// combines them exactly as it did before extraction.
struct VmEntryRun<R> {
	result: Result<R, String>,
	finalization: Result<(), String>,
	accounting: Result<(), String>,
	lifecycle: Vec<DeferredRuneLifecycle>,
	effects: HostEffects,
}

impl RuneHost {
	/// The shared VM-entry scaffold: remove the generation for the duration
	/// of the callback, gate on callback validity and retained-memory
	/// allowance, reset the host budget and effects, run the callback under
	/// the allocation limit, finalize deferred jobs inside the same limit
	/// scope, apply the signed retained delta, and restore the generation.
	///
	/// Deferred lifecycle actions are returned, not applied: they need the
	/// generation restored and `&mut self`, and their result folding is
	/// path-specific.
	fn enter_generation_callback<R>(
		&mut self,
		editor: &mut EditorHost,
		owner: RegistryOwnerId,
		callback_id: usize,
		call: impl FnOnce(&Function, &mut EditorHost) -> Result<R, String>,
	) -> Result<VmEntryRun<R>, VmEntryRejection> {
		let Some(mut generation) = self.generations.remove(&owner) else {
			return Err(VmEntryRejection::RetiredOwner);
		};
		let generation_bytes = generation.retained_bytes;
		let _unit_lifetime = &generation.unit;
		if generation.callbacks.get(callback_id).is_none() {
			self.generations.insert(owner, generation);
			return Err(VmEntryRejection::InvalidCallback);
		}
		let allowance = self.retained.callback_allowance(generation_bytes);
		if allowance == 0 {
			self.generations.insert(owner, generation);
			return Err(VmEntryRejection::AllowanceExhausted);
		}
		lock_services(&editor.services)
			.budget
			.reset(DEFAULT_HOST_CALL_QUOTA);
		editor.reset_effects();
		editor.begin_callback(owner);
		let ((result, lifecycle, finalization), delta) = limit::with(allowance, || {
			let remaining_before = limit::get();
			let result = {
				let callback = generation
					.callbacks
					.get(callback_id)
					.expect("callback was validated before VM entry");
				budget::with(CALLBACK_INSTRUCTION_BUDGET, || call(callback, editor)).call()
			};
			let config_result = if result.is_ok() {
				editor
					.finish_config_scope()
					.and_then(|operations| config_api::apply_operations(editor, operations))
			} else {
				let _ = editor.finish_config_scope();
				Ok(())
			};
			let commit_deferred = result.is_ok() && config_result.is_ok();
			let result = result.and_then(|value| config_result.map(|_| value));
			let (lifecycle, deferred_jobs) = editor.finish_callback();
			let finalization = Self::finalize_deferred_jobs(
				editor,
				&mut generation,
				owner,
				deferred_jobs,
				commit_deferred,
			);
			let remaining_after = limit::get();
			(
				(result, lifecycle, finalization),
				signed_delta(remaining_before, remaining_after),
			)
		})
		.call();
		let effects = editor.take_effects();
		let accounting = self.retained.apply_callback_delta(&mut generation, delta);
		self.generations.insert(owner, generation);
		Ok(VmEntryRun {
			result,
			finalization,
			accounting,
			lifecycle,
			effects,
		})
	}

	fn finalize_deferred_jobs(
		editor: &mut EditorHost,
		generation: &mut Generation,
		owner: RegistryOwnerId,
		jobs: Vec<DeferredRuneJob>,
		commit: bool,
	) -> Result<(), String> {
		let mut first_error = None;
		let mut may_commit = commit && editor.session.runtime_registries.owner_is_live(owner);
		for deferred in jobs {
			if !may_commit || editor.session.jobs.owner(deferred.reservation.id()) != Some(owner) {
				editor
					.session
					.jobs
					.discard_reservation(deferred.reservation);
				continue;
			}
			let callback_id = generation.callbacks.len();
			generation.callbacks.push(deferred.completion);
			let lease = ActionLease::new(owner, RuneCallbackRef { owner, callback_id });
			if let Err(error) = editor
				.session
				.jobs
				.commit_reserved_job(deferred.reservation, lease)
			{
				let dropped = generation.callbacks.pop();
				debug_assert!(
					dropped.is_some(),
					"just-pushed Rune callback must be present"
				);
				editor
					.session
					.jobs
					.discard_reservation(deferred.reservation);
				first_error.get_or_insert_with(|| error.to_string());
				may_commit = false;
			}
		}
		first_error.map_or(Ok(()), Err)
	}

	pub(crate) fn complete_job(
		&mut self,
		editor: &mut EditorHost,
		invocation: crate::engine::jobs::JobInvocation,
		completion: &JobCompletion,
	) -> SubscriberOutcome {
		let Some(callback) = invocation.action::<RuneCallbackRef>().copied() else {
			return SubscriberOutcome::failed(BoundedStatus::fixed("Rune job payload is missing"));
		};
		if callback.owner != invocation.owner() {
			// A lease from another owner must never enter that owner's VM.  This is
			// indistinguishable from an owner that was retired between completion
			// and delivery, so consume it silently without falling back elsewhere.
			return SubscriberOutcome::delivered();
		}
		let run = match self.enter_generation_callback(
			editor,
			callback.owner,
			callback.callback_id,
			|function, editor| {
				let job_result =
					crate::engine::rune_host::job_api::RuneJobResult::from_completion(completion);
				function
					.call::<Value>((
						&mut *editor,
						crate::engine::rune_host::job_api::RuneJob::from_invocation(&invocation),
						&job_result,
					))
					.into_result()
					.map_err(|error| render_error("Rune job callback error", &error))
					.and_then(decode_callback_unit)
			},
		) {
			Ok(run) => run,
			Err(VmEntryRejection::RetiredOwner) => return SubscriberOutcome::delivered(),
			Err(VmEntryRejection::InvalidCallback) => {
				return SubscriberOutcome::failed(BoundedStatus::fixed(
					"Rune job callback ID is invalid",
				));
			}
			Err(VmEntryRejection::AllowanceExhausted) => {
				return SubscriberOutcome::failed(BoundedStatus::fixed(
					"Rune job retained-memory limit is exhausted",
				));
			}
		};
		let effects = run.effects;
		let lifecycle = self.apply_deferred_lifecycle(editor, run.lifecycle);
		match run
			.result
			.and(run.finalization)
			.and(run.accounting)
			.and_then(|_| lifecycle.map(drop))
		{
			Ok(()) => SubscriberOutcome {
				needs_render: effects.needs_render,
				foreground_status: None,
				result: SubscriberResult::Delivered,
			},
			Err(error) => {
				lock_services(&self.services).messages.push_line(&error);
				SubscriberOutcome {
					needs_render: true,
					foreground_status: Some(
						BoundedStatus::new(bounded_status(&error)).expect("bounded status"),
					),
					result: SubscriberResult::Failed,
				}
			}
		}
	}
	pub(crate) fn new(
		config: RuneHostConfig,
		services: SharedHostServices,
		internal: InternalSender,
	) -> Result<Self, String> {
		validate_source_path_input(&config.init_script)?;
		let configured_init_path = normalize_path(&config.init_script)?;
		let built = limit::with(COMPILE_ALLOCATION_CAP, || build_context(services.clone())).call();
		let (context, runtime) = built?;
		Ok(Self {
			config,
			configured_init_path,
			context: Some(context),
			runtime: Arc::new(runtime),
			services,
			generations: BTreeMap::new(),
			sources: BTreeMap::new(),
			next_source_id: SourceId::new(1),
			next_source_request_id: SourceRequestId::FIRST,
			retained: RetainedRuneBudget::new(),
			internal,
			compiler: None,
			pending_compile: None,
			queued_order: VecDeque::new(),
			queued_sources: BTreeMap::new(),
			source_key_bytes: 0,
			autocmd_failures: BTreeMap::new(),
		})
	}

	pub(crate) fn run_init(&mut self, editor: &mut EditorHost) -> StartupScriptReport {
		let source = match read_source(&self.config.init_script) {
			Ok(Some(source)) => source,
			Ok(None) => return StartupScriptReport::default(),
			Err(error) => return self.report_error(error),
		};
		let request = match self.allocate_source_request() {
			Ok(request) => request,
			Err(error) => return self.report_error(error),
		};
		let source_id = match self.allocate_source_id() {
			Ok(source_id) => source_id,
			Err(error) => return self.report_error(error),
		};
		self.sources.insert(
			SourceKey::Init,
			SourceSlot {
				id: source_id,
				current_owner: None,
				latest_request: request,
			},
		);
		let path = self.config.init_script.clone();
		let reservation = match self.retained.reserve_compile() {
			Ok(reservation) => reservation,
			Err(error) => return self.fail_initial_source(error),
		};
		let compiled = limit::with(reservation, || {
			compile_source(
				self.context
					.as_ref()
					.expect("build context remains available before worker startup"),
				&path,
				&source,
			)
		})
		.call();
		let compiled = match compiled {
			Ok(compiled) => {
				if let Err(error) = self
					.retained
					.retain_compiled(reservation, compiled.retained_bytes)
				{
					return self.fail_initial_source(error);
				}
				compiled
			}
			Err(error) => return self.fail_initial_source(error),
		};
		let (published, effects) = self.publish_compiled(
			SourceKey::Init,
			compiled,
			editor,
			STARTUP_INSTRUCTION_BUDGET,
		);
		match published {
			Ok(output) => StartupScriptReport {
				error: None,
				output,
				needs_render: effects.needs_render,
			},
			Err(error) => {
				let mut report = self.fail_initial_source(error);
				report.needs_render = effects.needs_render;
				report
			}
		}
	}

	fn publish_compiled(
		&mut self,
		key: SourceKey,
		compiled: CompiledUnit,
		editor: &mut EditorHost,
		instruction_budget: usize,
	) -> (Result<Option<String>, String>, HostEffects) {
		editor.reset_effects();
		let result = self.publish_compiled_inner(key, compiled, editor, instruction_budget);
		let effects = editor.take_effects();
		(result, effects)
	}

	fn publish_compiled_inner(
		&mut self,
		key: SourceKey,
		compiled: CompiledUnit,
		editor: &mut EditorHost,
		instruction_budget: usize,
	) -> Result<Option<String>, String> {
		for warning in &compiled.warnings {
			lock_services(&self.services).messages.push_line(warning);
		}
		let current_owner = self.sources.get(&key).and_then(|slot| slot.current_owner);
		let retired_bytes = current_owner
			.and_then(|owner| self.generations.get(&owner))
			.map_or(0, |generation| generation.retained_bytes);
		let owner = editor
			.session
			.runtime_registries
			.allocate_owner()
			.map_err(|error| error.to_string())?;
		editor.staging = Some(SourceStaging::new(owner));
		let before = {
			let mut services = lock_services(&editor.services);
			services.budget.reset(DEFAULT_HOST_CALL_QUOTA);
			services.messages.sequence()
		};
		let allowance = self
			.retained
			.evaluation_allowance(compiled.retained_bytes)
			.inspect_err(|_| {
				editor.staging = None;
				let _ = editor
					.session
					.runtime_registries
					.discard_unpublished_empty_owner(owner);
			})?;
		let unit = Arc::new(compiled.unit);
		let (mut result, evaluation_delta) = limit::with(allowance, || {
			let remaining_before = limit::get();
			let mut vm = Vm::new(self.runtime.clone(), unit.clone());
			let result = budget::with(instruction_budget, || vm.call(["main"], (&mut *editor,)))
				.call()
				.map_err(|error| render_error("Rune runtime error", &error))
				.and_then(decode_startup_return);
			drop(vm);
			let remaining_after = limit::get();
			(result, signed_delta(remaining_before, remaining_after))
		})
		.call();
		let config_result = if result.is_ok() {
			editor
				.finish_config_scope()
				.and_then(|operations| config_api::apply_operations(editor, operations))
		} else {
			let _ = editor.finish_config_scope();
			Ok(())
		};
		result = result.and(config_result);
		let staging = editor.staging.take();
		let cleanup = |editor: &mut EditorHost| {
			let _ = editor
				.session
				.runtime_registries
				.discard_unpublished_empty_owner(owner);
		};
		if let Err(error) = result {
			drop(staging);
			cleanup(editor);
			return Err(error);
		}
		let evaluation_bytes = match usize::try_from(evaluation_delta) {
			Ok(bytes) => bytes,
			Err(_) => {
				drop(staging);
				cleanup(editor);
				return Err("Rune source evaluation released uncharged memory".to_string());
			}
		};
		let staging = staging.ok_or_else(|| {
			cleanup(editor);
			"Rune source staging was lost".to_string()
		})?;
		let (batch, callbacks) = staging.into_batch();
		let retained_bytes = self
			.retained
			.admit_generation(compiled.retained_bytes, evaluation_bytes, retired_bytes)
			.inspect_err(|_| {
				cleanup(editor);
			})?;
		let publish = if let Some(current_owner) = current_owner {
			editor
				.session
				.replace_runtime_owner_generation(current_owner, batch)
		} else {
			editor.session.runtime_registries.commit_owner_batch(batch)
		};
		publish.map_err(|error| {
			cleanup(editor);
			error.to_string()
		})?;
		self.retained
			.commit_generation(retained_bytes, retired_bytes);
		self.generations.insert(
			owner,
			Generation {
				unit,
				callbacks,
				retained_bytes,
			},
		);
		if let Some(current_owner) = current_owner {
			self.generations.remove(&current_owner);
			self.autocmd_failures.remove(&current_owner);
		}
		let source = {
			let slot = self
				.sources
				.get_mut(&key)
				.expect("source slot must exist through publication");
			slot.current_owner = Some(owner);
			slot.id
		};
		if let Some(current_owner) = current_owner {
			editor.session.events.push(EngineEvent::SourceUnloaded {
				source,
				owner: current_owner,
			});
		}
		editor
			.session
			.events
			.push(EngineEvent::SourceLoaded { source, owner });
		Ok(lock_services(&self.services).messages.latest_since(before))
	}

	pub(crate) fn request_source(&mut self, path: PathBuf) -> Result<String, String> {
		validate_source_path_input(&path)?;
		let (key, path) = self.normalize_source(path)?;
		let is_new = !self.sources.contains_key(&key);
		let previous_request = self.sources.get(&key).map(|slot| slot.latest_request);
		let key_bytes = source_key_bytes(&key);
		if is_new {
			validate_source_key_admission(self.sources.len(), self.source_key_bytes, key_bytes)?;
		}
		let request = self.allocate_source_request()?;
		if is_new {
			let source_id = self.allocate_source_id()?;
			self.sources.insert(
				key.clone(),
				SourceSlot {
					id: source_id,
					current_owner: None,
					latest_request: request,
				},
			);
			self.source_key_bytes += key_bytes;
		} else {
			self.sources
				.get_mut(&key)
				.expect("existing source key must remain live")
				.latest_request = request;
		}
		if self.pending_compile.is_some() {
			if !self.queued_sources.contains_key(&key) {
				self.queued_order.push_back(key.clone());
			}
			self.queued_sources
				.insert(key, QueuedSource { request, path });
		} else if let Err(error) = self.dispatch_compile(key.clone(), request, path) {
			if is_new {
				self.remove_empty_source_slot(&key);
			} else if let Some(previous_request) = previous_request {
				self.sources
					.get_mut(&key)
					.expect("existing source slot must survive dispatch failure")
					.latest_request = previous_request;
			}
			return Err(error);
		}
		Ok("Sourcing Rune file…".to_string())
	}

	pub(crate) fn apply_compile_result(
		&mut self,
		editor: &mut EditorHost,
		result: SourceCompileResult,
	) -> SourceCompileReport {
		// A compiler completion has always requested a render; host effects are
		// merged into that existing policy rather than being inferred from status.
		let mut needs_render = true;
		let pending = self.pending_compile.take();
		let mut status = match pending {
			Some(pending)
				if pending.key == result.key
					&& pending.request == result.request
					&& pending.reservation == result.reservation =>
			{
				match result.result {
					Ok(compiled) => {
						if let Err(error) = self
							.retained
							.retain_compiled(result.reservation, compiled.retained_bytes)
						{
							self.retained.release_reservation();
							Some(error)
						} else if self
							.sources
							.get(&result.key)
							.is_none_or(|slot| slot.latest_request != result.request)
						{
							self.retained.release_reservation();
							None
						} else {
							let (published, effects) = self.publish_compiled(
								result.key.clone(),
								compiled,
								editor,
								STARTUP_INSTRUCTION_BUDGET,
							);
							needs_render |= effects.needs_render;
							match published {
								Ok(output) => {
									output.or_else(|| Some("Rune source loaded".to_string()))
								}
								Err(error) => {
									self.retained.release_reservation();
									self.remove_empty_source_slot(&result.key);
									Some(error)
								}
							}
						}
					}
					Err(error) => {
						self.retained.release_reservation();
						self.remove_empty_source_slot(&result.key);
						Some(error)
					}
				}
			}
			_ => {
				self.retained.release_reservation();
				Some("Discarded an unexpected Rune compiler result".to_string())
			}
		};
		if let Some(compiler) = &self.compiler {
			compiler.acknowledge();
		}
		if let Err(error) = self.dispatch_next_queued() {
			status = Some(error);
		}
		if let Some(message) = &status {
			lock_services(&self.services).messages.push_line(message);
		}
		SourceCompileReport {
			status: status.map(|message| bounded_status(&message)),
			needs_render,
		}
	}

	pub(crate) fn unload_source(
		&mut self,
		path: PathBuf,
		editor: &mut EditorHost,
	) -> Result<String, String> {
		validate_source_path_input(&path)?;
		let (key, _) = self.normalize_source(path)?;
		let Some(slot) = self.sources.get(&key) else {
			return Err("Rune source is not loaded".to_string());
		};
		let source = slot.id;
		let unloaded_owner = slot.current_owner;
		if let Some(owner) = slot.current_owner {
			let retained_bytes = self
				.generations
				.get(&owner)
				.ok_or_else(|| "Rune generation is missing".to_string())?
				.retained_bytes;
			let charged_after = self
				.retained
				.charged
				.checked_sub(retained_bytes)
				.ok_or_else(|| "Rune retained-memory accounting underflowed".to_string())?;
			editor
				.session
				.remove_runtime_owner(owner)
				.map_err(|error| error.to_string())?;
			let generation = self
				.generations
				.remove(&owner)
				.expect("generation was preflighted before owner removal");
			debug_assert_eq!(generation.retained_bytes, retained_bytes);
			self.retained.charged = charged_after;
			self.autocmd_failures.remove(&owner);
		}
		self.queued_sources.remove(&key);
		self.queued_order.retain(|queued| queued != &key);
		self.remove_source_slot(&key);
		if let Some(owner) = unloaded_owner {
			editor
				.session
				.events
				.push(EngineEvent::SourceUnloaded { source, owner });
		}
		Ok("Rune source unloaded".to_string())
	}

	fn dispatch_compile(
		&mut self,
		key: SourceKey,
		request: SourceRequestId,
		path: PathBuf,
	) -> Result<(), String> {
		if self.compiler.is_none() {
			let context = self
				.context
				.take()
				.ok_or_else(|| "Rune build context is unavailable".to_string())?;
			self.compiler = Some(SourceCompiler::new(context, self.internal.clone())?);
		}
		let reservation = self.retained.reserve_compile()?;
		if let Err(error) = self
			.compiler
			.as_ref()
			.expect("compiler was initialized")
			.submit(key.clone(), request, path, reservation)
		{
			self.retained.release_reservation();
			return Err(error);
		}
		self.pending_compile = Some(PendingCompile {
			key,
			request,
			reservation,
		});
		Ok(())
	}

	fn dispatch_next_queued(&mut self) -> Result<(), String> {
		while let Some(key) = self.queued_order.pop_front() {
			let Some(queued) = self.queued_sources.remove(&key) else {
				continue;
			};
			if self
				.sources
				.get(&key)
				.is_none_or(|slot| slot.latest_request != queued.request)
			{
				continue;
			}
			let result = self.dispatch_compile(key.clone(), queued.request, queued.path);
			if result.is_err() {
				self.remove_empty_source_slot(&key);
			}
			return result;
		}
		Ok(())
	}

	fn normalize_source(&self, path: PathBuf) -> Result<(SourceKey, PathBuf), String> {
		let normalized = normalize_path(&path)?;
		let key = if normalized == self.configured_init_path {
			SourceKey::Init
		} else {
			SourceKey::Path(normalized.clone())
		};
		Ok((key, normalized))
	}

	fn remove_empty_source_slot(&mut self, key: &SourceKey) {
		if self
			.sources
			.get(key)
			.is_some_and(|slot| slot.current_owner.is_none())
		{
			self.remove_source_slot(key);
		}
	}

	fn remove_source_slot(&mut self, key: &SourceKey) {
		if self.sources.remove(key).is_some() {
			self.source_key_bytes = self.source_key_bytes.saturating_sub(source_key_bytes(key));
		}
	}

	fn allocate_source_request(&mut self) -> Result<SourceRequestId, String> {
		let request = self.next_source_request_id;
		self.next_source_request_id = request
			.next()
			.ok_or_else(|| "Rune source request IDs are exhausted".to_string())?;
		Ok(request)
	}

	fn allocate_source_id(&mut self) -> Result<SourceId, String> {
		if self.next_source_id.get() == u64::MAX {
			return Err("Rune source IDs are exhausted".to_string());
		}
		let source = self.next_source_id;
		self.next_source_id = source.next();
		Ok(source)
	}

	fn fail_initial_source(&mut self, error: String) -> StartupScriptReport {
		self.retained.release_reservation();
		if self
			.sources
			.get(&SourceKey::Init)
			.is_some_and(|slot| slot.current_owner.is_none())
		{
			self.sources.remove(&SourceKey::Init);
		}
		self.report_error(error)
	}

	fn report_error(&self, error: String) -> StartupScriptReport {
		lock_services(&self.services).messages.push_line(&error);
		StartupScriptReport {
			error: Some(bounded_status(&error)),
			output: None,
			needs_render: false,
		}
	}

	fn apply_deferred_lifecycle(
		&mut self,
		editor: &mut EditorHost,
		actions: Vec<DeferredRuneLifecycle>,
	) -> Result<Option<String>, String> {
		let mut status = None;
		let mut first_error = None;
		for action in actions {
			let result = match action {
				DeferredRuneLifecycle::Source(path) => self.request_source(path),
				DeferredRuneLifecycle::Unload(path) => self.unload_source(path, editor),
			};
			match result {
				Ok(message) => status = Some(message),
				Err(error) if first_error.is_none() => first_error = Some(error),
				Err(_) => {}
			}
		}
		if let Some(error) = first_error {
			return Err(error);
		}
		Ok(status)
	}
}

impl BaselineContributor for RuneHost {
	fn contribute(&self, builder: &mut BaselineBuilder) -> Result<(), BaselineError> {
		let mut live = Vec::new();
		live.try_reserve(self.sources.len())
			.map_err(|_| BaselineError::AllocationFailed {
				domain: BaselineDomain::Sources,
			})?;
		live.extend(
			self.sources
				.values()
				.filter_map(|slot| slot.current_owner.map(|owner| (slot.id, owner))),
		);
		live.sort_unstable_by_key(|(source, _)| *source);
		for (source, owner) in live {
			builder.push_source(source, owner)?;
		}
		Ok(())
	}
}

enum AutocmdArgument<'a> {
	Baseline(&'a RuneAutocmdBaseline),
	Event(&'a RuneAutocmdEvent),
}

impl RuneHost {
	fn validate_autocmd_ref(
		invocation: &SubscriberInvocation,
		callback: RuneAutocmdRef,
	) -> Result<RuneAutocmdRef, String> {
		let owner = invocation.owner();
		if callback.initialize.owner != owner || callback.event.owner != owner {
			return Err("Rune autocmd callback owner does not match its invocation".to_string());
		}
		Ok(callback)
	}

	fn invoke_autocmd_function(
		&mut self,
		editor: &mut EditorHost,
		callback_ref: RuneCallbackRef,
		argument: AutocmdArgument<'_>,
	) -> (Result<(), String>, HostEffects) {
		let run = match self.enter_generation_callback(
			editor,
			callback_ref.owner,
			callback_ref.callback_id,
			|callback, editor| {
				let value = match argument {
					AutocmdArgument::Baseline(baseline) => {
						callback.call::<Value>((&mut *editor, baseline))
					}
					AutocmdArgument::Event(event) => callback.call::<Value>((&mut *editor, event)),
				}
				.into_result()
				.map_err(|error| render_error("Rune autocmd callback error", &error))?;
				decode_callback_unit(value)
			},
		) {
			Ok(run) => run,
			Err(VmEntryRejection::RetiredOwner) => {
				return (
					Err("Rune autocmd owner is retired".to_string()),
					HostEffects::default(),
				);
			}
			Err(VmEntryRejection::InvalidCallback) => {
				return (
					Err("Rune autocmd callback ID is invalid".to_string()),
					HostEffects::default(),
				);
			}
			Err(VmEntryRejection::AllowanceExhausted) => {
				return (
					Err("Rune autocmd retained-memory limit is exhausted".to_string()),
					HostEffects::default(),
				);
			}
		};
		let lifecycle_result = self.apply_deferred_lifecycle(editor, run.lifecycle);
		if let Ok(Some(message)) = &lifecycle_result {
			lock_services(&self.services).messages.push_line(message);
		}

		let result = run
			.result
			.and(run.finalization)
			.and(run.accounting)
			.and_then(|()| lifecycle_result.map(drop));
		(result, run.effects)
	}

	fn autocmd_failure_outcome(
		&mut self,
		editor: &EditorHost,
		invocation: &SubscriberInvocation,
		error: String,
		effects: HostEffects,
	) -> SubscriberOutcome {
		let owner = invocation.owner();
		let id = invocation.id();
		let still_live = editor
			.session
			.runtime_registries
			.subscription_is_owned_by(id, owner)
			&& self.generations.contains_key(&owner);
		let failures = if still_live {
			let failures = self
				.autocmd_failures
				.entry(owner)
				.or_default()
				.entry(id)
				.or_default();
			*failures = failures.saturating_add(1);
			Some(*failures)
		} else {
			None
		};
		lock_services(&self.services).messages.push_line(&error);
		let foreground =
			failures.is_none_or(|count| count == 1 || count == MAX_AUTOCMD_CONSECUTIVE_FAILURES);
		let status = foreground.then(|| {
			let message = if failures == Some(MAX_AUTOCMD_CONSECUTIVE_FAILURES) {
				format!("Rune autocmd muted after repeated failures: {error}")
			} else {
				error.clone()
			};
			BoundedStatus::new(bounded_status(&message))
				.expect("bounded Rune autocmd status must fit the subscriber status cap")
		});
		SubscriberOutcome {
			needs_render: effects.needs_render || status.is_some(),
			foreground_status: status,
			result: SubscriberResult::Failed,
		}
	}
}

impl SubscriberDispatcher<EditorHost> for RuneHost {
	fn owner_is_live(&self, owner: RegistryOwnerId) -> bool {
		self.generations.contains_key(&owner)
	}

	fn initialize(
		&mut self,
		editor: &mut EditorHost,
		invocation: SubscriberInvocation,
		baseline: &Arc<SubscriberBaseline>,
	) -> SubscriberOutcome {
		let Some(callback) = invocation.action::<RuneAutocmdRef>().copied() else {
			return SubscriberOutcome::failed(BoundedStatus::fixed(
				"Rune autocmd payload is missing",
			));
		};
		let callback = match Self::validate_autocmd_ref(&invocation, callback) {
			Ok(callback) => callback,
			Err(error) => {
				return SubscriberOutcome::failed(
					BoundedStatus::new(bounded_status(&error))
						.expect("bounded Rune autocmd status must fit"),
				);
			}
		};
		let baseline = RuneAutocmdBaseline::new(Arc::clone(baseline));
		let (result, effects) = self.invoke_autocmd_function(
			editor,
			callback.initialize,
			AutocmdArgument::Baseline(&baseline),
		);
		match result {
			Ok(()) => SubscriberOutcome {
				needs_render: effects.needs_render,
				foreground_status: None,
				result: SubscriberResult::Delivered,
			},
			Err(error) => {
				lock_services(&self.services).messages.push_line(&error);
				SubscriberOutcome {
					needs_render: true,
					foreground_status: Some(
						BoundedStatus::new(bounded_status(&error))
							.expect("bounded Rune autocmd status must fit"),
					),
					result: SubscriberResult::Failed,
				}
			}
		}
	}

	fn deliver(
		&mut self,
		editor: &mut EditorHost,
		invocation: SubscriberInvocation,
		event: &EngineEvent,
	) -> SubscriberOutcome {
		let Some(callback) = invocation.action::<RuneAutocmdRef>().copied() else {
			return SubscriberOutcome::failed(BoundedStatus::fixed(
				"Rune autocmd payload is missing",
			));
		};
		let callback = match Self::validate_autocmd_ref(&invocation, callback) {
			Ok(callback) => callback,
			Err(error) => {
				return SubscriberOutcome::failed(
					BoundedStatus::new(bounded_status(&error))
						.expect("bounded Rune autocmd status must fit"),
				);
			}
		};
		if callback
			.buffer_filter
			.is_some_and(|filter| !event_matches_buffer(event, filter))
		{
			return SubscriberOutcome::delivered();
		}
		if self
			.autocmd_failures
			.get(&invocation.owner())
			.and_then(|failures| failures.get(&invocation.id()))
			.is_some_and(|failures| *failures >= MAX_AUTOCMD_CONSECUTIVE_FAILURES)
		{
			return SubscriberOutcome::delivered();
		}

		let event = RuneAutocmdEvent::new(event);
		let (result, effects) =
			self.invoke_autocmd_function(editor, callback.event, AutocmdArgument::Event(&event));
		match result {
			Ok(()) => {
				if editor
					.session
					.runtime_registries
					.subscription_is_owned_by(invocation.id(), invocation.owner())
					&& let Some(failures) = self.autocmd_failures.get_mut(&invocation.owner())
				{
					failures.remove(&invocation.id());
					if failures.is_empty() {
						self.autocmd_failures.remove(&invocation.owner());
					}
				}
				SubscriberOutcome {
					needs_render: effects.needs_render,
					foreground_status: None,
					result: SubscriberResult::Delivered,
				}
			}
			Err(error) => self.autocmd_failure_outcome(editor, &invocation, error, effects),
		}
	}
}

fn bounded_status(message: &str) -> String {
	if message.len() <= MAX_STATUS_BYTES {
		return message.to_string();
	}
	let marker = "…";
	let mut end = MAX_STATUS_BYTES.saturating_sub(marker.len());
	while end > 0 && !message.is_char_boundary(end) {
		end -= 1;
	}
	let mut bounded = message[..end].to_string();
	bounded.push_str(marker);
	bounded
}

impl DynamicActionExecutor<EditorHost> for RuneHost {
	type Output = DynamicActionOutcome;

	fn owner_is_live(&self, owner: RegistryOwnerId) -> bool {
		self.generations.contains_key(&owner)
	}

	fn invoke(
		&mut self,
		editor: &mut EditorHost,
		invocation: InvocationSnapshot,
	) -> Result<Self::Output, ActionError> {
		let callback_ref = invocation
			.action::<RuneCallbackRef>()
			.copied()
			.ok_or_else(|| ActionError::Failed("Rune callback payload is missing".to_string()))?;
		if callback_ref.owner != invocation.owner() {
			return Err(ActionError::Failed(
				"Rune callback owner does not match its invocation".to_string(),
			));
		}
		let before_messages = lock_services(&editor.services).messages.sequence();
		let invocation_kind = invocation.invocation;
		let run = match self.enter_generation_callback(
			editor,
			callback_ref.owner,
			callback_ref.callback_id,
			|callback, editor| call_rune_callback(callback, editor, &invocation_kind),
		) {
			Ok(run) => run,
			Err(VmEntryRejection::RetiredOwner) => {
				return Err(ActionError::RetiredOwner(callback_ref.owner));
			}
			Err(VmEntryRejection::InvalidCallback) => {
				return Err(ActionError::Failed(
					"Rune callback ID is invalid".to_string(),
				));
			}
			Err(VmEntryRejection::AllowanceExhausted) => {
				return Err(ActionError::Failed(
					"Rune callback retained-memory limit is exhausted".to_string(),
				));
			}
		};
		run.accounting.map_err(ActionError::Failed)?;
		run.finalization.map_err(ActionError::Failed)?;
		let lifecycle_result = self.apply_deferred_lifecycle(editor, run.lifecycle);

		let mut outcome = run.result.map_err(ActionError::Failed)?;
		outcome.needs_render |= run.effects.needs_render;
		let lifecycle_status = lifecycle_result.map_err(ActionError::Failed)?;
		if outcome.status_message.is_none() && outcome.option_query_display.is_none() {
			outcome.status_message = lifecycle_status.or_else(|| {
				lock_services(&self.services)
					.messages
					.latest_since(before_messages)
			});
			outcome.needs_render |= outcome.status_message.is_some();
		}
		Ok(outcome)
	}
}

fn call_rune_callback(
	callback: &Function,
	editor: &mut EditorHost,
	invocation: &DynamicInvocation,
) -> Result<DynamicActionOutcome, String> {
	let (value, option_query) = match invocation {
		DynamicInvocation::Command { args, bang, .. } => (
			callback
				.call::<Value>((&mut *editor, args.clone(), *bang))
				.into_result()
				.map_err(|error| render_error("Rune callback error", &error))?,
			false,
		),
		DynamicInvocation::Mapping { .. } => (
			callback
				.call::<Value>((&mut *editor,))
				.into_result()
				.map_err(|error| render_error("Rune callback error", &error))?,
			false,
		),
		DynamicInvocation::Option { operation, .. } => {
			let (call, query) = match operation {
				OptionInvocation::Query => (OptionCall::Query, true),
				OptionInvocation::Set(OptionValue::Bool(value)) => {
					(OptionCall::SetBool(*value), false)
				}
				OptionInvocation::Set(OptionValue::Integer(value)) => {
					(OptionCall::SetInteger(*value), false)
				}
				OptionInvocation::Set(OptionValue::String(value)) => {
					(OptionCall::SetString(value.clone()), false)
				}
			};
			(
				callback
					.call::<Value>((&mut *editor, call))
					.into_result()
					.map_err(|error| render_error("Rune callback error", &error))?,
				query,
			)
		}
	};
	decode_callback_value(value, option_query)
}

fn decode_startup_return(value: Value) -> Result<(), String> {
	if value.into_unit().is_ok() {
		return Ok(());
	}
	let result = value
		.downcast::<Result<Value, Value>>()
		.map_err(|_| "Rune init main must return () or Result<(), String>".to_string())?;
	match result {
		Ok(_) => Ok(()),
		Err(error) => {
			let message = error
				.into_string()
				.map_err(|_| "Rune init main returned a non-string error".to_string())?;
			Err(message.to_string())
		}
	}
}

fn decode_callback_value(value: Value, option_query: bool) -> Result<DynamicActionOutcome, String> {
	let value = decode_callback_result(value)?;
	if let Ok(string) = value.borrow_string_ref() {
		if string.len() > MAX_STATUS_BYTES {
			return Err(format!(
				"Rune callback returned {} bytes; the limit is {MAX_STATUS_BYTES}",
				string.len()
			));
		}
		let string = string.to_string();
		return Ok(if option_query {
			DynamicActionOutcome {
				option_query_display: Some(string),
				needs_render: true,
				..DynamicActionOutcome::default()
			}
		} else {
			DynamicActionOutcome {
				status_message: Some(string),
				needs_render: true,
				..DynamicActionOutcome::default()
			}
		});
	}
	value
		.into_unit()
		.map_err(|error| render_error("Rune callback must return () or String", &error))?;
	if option_query {
		return Err("Rune option query must return a String".to_string());
	}
	Ok(DynamicActionOutcome::default())
}

fn decode_callback_unit(value: Value) -> Result<(), String> {
	decode_callback_result(value)?
		.into_unit()
		.map_err(|error| render_error("Rune callback must return () or Result<(), String>", &error))
}

fn decode_callback_result(value: Value) -> Result<Value, String> {
	let original = value.clone();
	match value.downcast::<Result<Value, Value>>() {
		Ok(Ok(value)) => Ok(value),
		Ok(Err(error)) => {
			let message = error
				.into_string()
				.map_err(|_| "Rune callback returned a non-string error".to_string())?;
			Err(message.to_string())
		}
		Err(_) => Ok(original),
	}
}

fn build_context(services: SharedHostServices) -> Result<(Context, RuntimeContext), String> {
	let mut context =
		Context::with_config(false).map_err(|error| render_error("Rune context error", &error))?;
	let mut baryon =
		Module::with_crate("baryon").map_err(|error| render_error("Rune module error", &error))?;
	baryon
		.ty::<EditorHost>()
		.map_err(|error| render_error("Rune module error", &error))?;
	buffer_api::install(&mut baryon).map_err(|error| render_error("Rune module error", &error))?;
	config_api::install(&mut baryon).map_err(|error| render_error("Rune module error", &error))?;
	job_api::install(&mut baryon).map_err(|error| render_error("Rune module error", &error))?;
	autocmd::install(&mut baryon).map_err(|error| render_error("Rune module error", &error))?;
	baryon
		.ty::<OptionCall>()
		.map_err(|error| render_error("Rune module error", &error))?;
	baryon
		.function("command", register_command)
		.build()
		.map_err(|error| render_error("Rune module error", &error))?;
	baryon
		.function("option", register_option)
		.build()
		.map_err(|error| render_error("Rune module error", &error))?;
	baryon
		.function("map", register_mapping)
		.build()
		.map_err(|error| render_error("Rune module error", &error))?;
	baryon
		.function("source", defer_source)
		.build()
		.map_err(|error| render_error("Rune module error", &error))?;
	baryon
		.function("unload", defer_unload)
		.build()
		.map_err(|error| render_error("Rune module error", &error))?;
	#[cfg(test)]
	baryon
		.function(
			"test_set_status",
			|editor: &mut EditorHost, status: &str| {
				editor.session.ui.status_message = Some(status.to_string());
				editor.mark_needs_render();
			},
		)
		.build()
		.map_err(|error| render_error("Rune module error", &error))?;
	#[cfg(test)]
	baryon
		.function("test_assert_init_before_file", |editor: &mut EditorHost| {
			let unnamed = editor
				.session
				.layout
				.find_pane(editor.session.focused_pane_id)
				.and_then(|pane| pane.tabs.get(pane.active_tab_index))
				.and_then(|view| editor.session.buffers.get(view.buffer_id))
				.is_some_and(|buffer| buffer.file_path().is_none());
			if unnamed {
				VmResult::Ok(())
			} else {
				VmResult::panic("init.rn ran after the initial file load")
			}
		})
		.build()
		.map_err(|error| render_error("Rune module error", &error))?;
	#[cfg(test)]
	baryon
		.function(
			"test_seed_event_overflow_once",
			|editor: &mut EditorHost| {
				const SEEDED: &str = "rune-autocmd-overflow-seeded";
				editor.mark_needs_render();
				if editor.session.ui.status_message.as_deref() == Some(SEEDED) {
					return false;
				}
				for index in 0..=crate::engine::event_bus::MAX_PENDING_EVENTS {
					editor.session.events.push(EngineEvent::BufferOpened {
						buffer: crate::core::BufferId::new(index as u64 + 10_000),
						state: crate::core::StateId::new(index as u64 + 1),
					});
				}
				editor.session.ui.status_message = Some(SEEDED.to_string());
				true
			},
		)
		.build()
		.map_err(|error| render_error("Rune module error", &error))?;
	context
		.install(baryon)
		.map_err(|error| render_error("Rune context error", &error))?;
	context
		.install(io_module(services)?)
		.map_err(|error| render_error("Rune context error", &error))?;
	let runtime = context
		.runtime()
		.map_err(|error| render_error("Rune runtime context error", &error))?;
	Ok((context, runtime))
}

fn defer_source(editor: &mut EditorHost, path: &str) -> VmResult<()> {
	defer_lifecycle(editor, path, false)
}

fn defer_unload(editor: &mut EditorHost, path: &str) -> VmResult<()> {
	defer_lifecycle(editor, path, true)
}

fn defer_lifecycle(editor: &mut EditorHost, path: &str, unload: bool) -> VmResult<()> {
	if let Err(error) = lock_services(&editor.services).budget.charge() {
		return VmResult::panic(error);
	}
	match editor.defer_source_path(path, unload) {
		Ok(()) => VmResult::Ok(()),
		Err(error) => VmResult::panic(error),
	}
}

fn io_module(services: SharedHostServices) -> Result<Module, String> {
	let mut module = Module::with_crate_item("std", ["io"])
		.map_err(|error| render_error("Rune I/O module error", &error))?;
	let print_services = services.clone();
	module
		.function("print", move |message: &str| {
			write_message(&print_services, message, false)
		})
		.build()
		.map_err(|error| render_error("Rune I/O module error", &error))?;
	let println_services = services.clone();
	module
		.function("println", move |message: &str| {
			write_message(&println_services, message, true)
		})
		.build()
		.map_err(|error| render_error("Rune I/O module error", &error))?;
	module
		.raw_function(
			"dbg",
			move |stack: &mut dyn Memory,
			      address: InstAddress,
			      arguments: usize,
			      output: Output| {
				{
					let mut services = lock_services(&services);
					if let Err(error) = services.budget.charge() {
						return VmResult::panic(error);
					}
					if arguments > MAX_DBG_ARGUMENTS {
						return VmResult::panic("Rune dbg! argument limit is exceeded");
					}
					let values = match stack.slice_at(address, arguments) {
						Ok(values) => values,
						Err(error) => return VmResult::panic(error),
					};
					for value in values {
						let mut line = BoundedLine::default();
						let _ = write!(line, "{value:?}");
						services.messages.push_line(&line.finish());
					}
				}
				match output.store(stack, ()) {
					Ok(()) => VmResult::Ok(()),
					Err(error) => VmResult::panic(error),
				}
			},
		)
		.build()
		.map_err(|error| render_error("Rune I/O module error", &error))?;
	Ok(module)
}

fn write_message(services: &SharedHostServices, message: &str, newline: bool) -> VmResult<()> {
	let mut services = lock_services(services);
	if let Err(error) = services.budget.charge() {
		return VmResult::panic(error);
	}
	if message.len() > MAX_HOST_OUTPUT_CALL_BYTES {
		return VmResult::panic("Rune output call byte limit is exceeded");
	}
	services.messages.write(message, newline);
	VmResult::Ok(())
}

pub(super) fn read_source(path: &Path) -> Result<Option<String>, String> {
	let file = match File::open(path) {
		Ok(file) => file,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
		Err(error) => {
			return Err(render_error("Could not read Rune source", &error));
		}
	};
	let mut bytes = Vec::new();
	file.take((MAX_SOURCE_BYTES + 1) as u64)
		.read_to_end(&mut bytes)
		.map_err(|error| render_error("Could not read Rune source", &error))?;
	if bytes.len() > MAX_SOURCE_BYTES {
		return Err(format!(
			"Rune source exceeds the {} byte limit",
			MAX_SOURCE_BYTES
		));
	}
	String::from_utf8(bytes)
		.map(Some)
		.map_err(|_| "Rune source is not valid UTF-8".to_string())
}

pub(super) struct CompiledUnit {
	pub(super) unit: rune::Unit,
	pub(super) warnings: Vec<String>,
	pub(super) retained_bytes: usize,
}

pub(super) fn compile_source(
	context: &Context,
	path: &Path,
	source: &str,
) -> Result<CompiledUnit, String> {
	let remaining_before = limit::get();
	let mut sources = Sources::new();
	let source = Source::with_path("init", source, path)
		.map_err(|error| render_error("Rune source error", &error))?;
	sources
		.insert(source)
		.map_err(|error| render_error("Rune source error", &error))?;
	let mut diagnostics = Diagnostics::new();
	let result = rune::prepare(&mut sources)
		.with_context(context)
		.with_diagnostics(&mut diagnostics)
		.build();
	let rendered = render_diagnostics(&diagnostics);
	let has_error = diagnostics.has_error();
	drop(diagnostics);
	drop(sources);
	let retained_bytes = remaining_before.saturating_sub(limit::get());
	match result {
		Ok(unit) if !has_error => Ok(CompiledUnit {
			unit,
			warnings: rendered,
			retained_bytes,
		}),
		Ok(_) | Err(_) if !rendered.is_empty() => Err(rendered.join("\n")),
		Err(error) => Err(render_error("Rune compile error", &error)),
		Ok(unit) => Ok(CompiledUnit {
			unit,
			warnings: rendered,
			retained_bytes,
		}),
	}
}

fn signed_delta(before: usize, after: usize) -> i128 {
	before as i128 - after as i128
}

fn normalize_path(path: &Path) -> Result<PathBuf, String> {
	if let Ok(canonical) = std::fs::canonicalize(path) {
		return Ok(canonical);
	}
	let absolute = if path.is_absolute() {
		path.to_path_buf()
	} else {
		std::env::current_dir()
			.map_err(|error| format!("Could not resolve Rune source path: {error}"))?
			.join(path)
	};
	let mut normalized = PathBuf::new();
	for component in absolute.components() {
		match component {
			std::path::Component::CurDir => {}
			std::path::Component::ParentDir => {
				normalized.pop();
			}
			_ => normalized.push(component.as_os_str()),
		}
	}
	Ok(normalized)
}

fn source_key_bytes(key: &SourceKey) -> usize {
	match key {
		SourceKey::Init => 0,
		SourceKey::Path(path) => path.as_os_str().as_encoded_bytes().len(),
	}
}

fn validate_source_path_input(path: &Path) -> Result<(), String> {
	let bytes = path.as_os_str().as_encoded_bytes().len();
	if bytes > MAX_SOURCE_KEY_BYTES {
		return Err(format!(
			"Rune source path has {bytes} bytes; the limit is {MAX_SOURCE_KEY_BYTES}"
		));
	}
	Ok(())
}

fn validate_source_key_admission(
	current_slots: usize,
	current_bytes: usize,
	key_bytes: usize,
) -> Result<(), String> {
	if current_slots >= MAX_SOURCE_SLOTS {
		return Err(format!(
			"Rune source identity limit of {MAX_SOURCE_SLOTS} is reached"
		));
	}
	if key_bytes > MAX_SOURCE_KEY_BYTES {
		return Err(format!(
			"Rune source key has {key_bytes} bytes; the limit is {MAX_SOURCE_KEY_BYTES}"
		));
	}
	let total = current_bytes
		.checked_add(key_bytes)
		.ok_or_else(|| "Rune source-key accounting overflowed".to_string())?;
	if total > MAX_SOURCE_KEY_TOTAL_BYTES {
		return Err(format!(
			"Rune source keys exceed the {MAX_SOURCE_KEY_TOTAL_BYTES} byte session limit"
		));
	}
	Ok(())
}

#[derive(Default)]
struct BoundedLine {
	value: String,
	truncated: bool,
}

impl BoundedLine {
	fn finish(mut self) -> String {
		if self.truncated {
			let marker = "…";
			let target = MAX_MESSAGE_LINE_BYTES.saturating_sub(marker.len());
			let mut end = target.min(self.value.len());
			while end > 0 && !self.value.is_char_boundary(end) {
				end -= 1;
			}
			self.value.truncate(end);
			self.value.push_str(marker);
		}
		self.value
	}
}

impl Write for BoundedLine {
	fn write_str(&mut self, value: &str) -> fmt::Result {
		let remaining = MAX_MESSAGE_LINE_BYTES.saturating_sub(self.value.len());
		if remaining == 0 {
			self.truncated |= !value.is_empty();
			return Err(fmt::Error);
		}
		let mut keep = remaining.min(value.len());
		while keep > 0 && !value.is_char_boundary(keep) {
			keep -= 1;
		}
		self.value.push_str(&value[..keep]);
		if keep < value.len() {
			self.truncated = true;
			Err(fmt::Error)
		} else {
			Ok(())
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{
		MAX_RETAINED_PER_GENERATION, MAX_SOURCE_BYTES, MAX_SOURCE_KEY_BYTES,
		MAX_SOURCE_KEY_TOTAL_BYTES, MAX_SOURCE_SLOTS, PendingCompile, RetainedRuneBudget, RuneHost,
		RuneHostConfig, SESSION_RETAINED_CAP, SourceKey, SourceRequestId, SourceSlot,
		apply_signed_bytes, bounded_status, build_context, compile_source, read_source,
		validate_source_key_admission, validate_source_path_input, write_message,
	};
	use crate::core::{RegistryOwnerId, SourceId};
	use crate::ecs::UastRegistry;
	use crate::engine::event_bus::{EngineEvent, EventKind, EventKindSet};
	use crate::engine::event_subscribers::{
		BaselineBuilder, BaselineContributor, SubscriberBaseline, SubscriberDispatcher,
		SubscriberResult,
	};
	use crate::engine::rune_host::EditorHost;
	use crate::engine::rune_host::autocmd::{
		RuneAutocmdBaseline, RuneAutocmdEvent, RuneAutocmdRef,
	};
	use crate::engine::rune_host::facade::{HostServices, lock_services};
	use crate::engine::rune_host::registration::RuneCallbackRef;
	use crate::engine::runtime_registry::{
		ActionLease, OptionValue, RegistrationBatch, RuntimeSubscriptionRegistration,
	};
	use crate::engine::session::EditorSession;
	use crate::engine::settings::SaveDurability;
	use crate::engine::ui_colors::UiColorKey;
	use crate::engine::ui_colors::UiColorOverrides;
	use rune::Vm;
	use rune::runtime::budget;
	use rune::runtime::{Object, Vec as RuneVec};
	use std::collections::BTreeMap;
	use std::fs;
	use std::path::PathBuf;
	use std::sync::{Arc, Mutex};
	use std::time::{SystemTime, UNIX_EPOCH};

	fn temp_path(name: &str) -> PathBuf {
		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_nanos();
		std::env::temp_dir().join(format!("baryon-rune-{name}-{}-{nonce}", std::process::id()))
	}

	#[test]
	fn source_baseline_contribution_is_live_only_and_sorted_by_source_id() {
		assert_eq!(
			crate::engine::event_subscribers::MAX_BASELINE_SOURCES,
			MAX_SOURCE_SLOTS
		);
		let (_commands, _input, internal, _requests) = crate::engine::request::channel();
		let mut host = RuneHost::new(
			RuneHostConfig::new(temp_path("baseline-init")),
			Arc::new(Mutex::new(HostServices::default())),
			internal,
		)
		.expect("Rune host");
		host.sources.insert(
			SourceKey::Path(PathBuf::from("a")),
			SourceSlot {
				id: SourceId::new(3),
				current_owner: Some(RegistryOwnerId::new(30)),
				latest_request: SourceRequestId::FIRST,
			},
		);
		host.sources.insert(
			SourceKey::Path(PathBuf::from("z")),
			SourceSlot {
				id: SourceId::new(1),
				current_owner: Some(RegistryOwnerId::new(10)),
				latest_request: SourceRequestId::FIRST,
			},
		);
		host.sources.insert(
			SourceKey::Path(PathBuf::from("pending")),
			SourceSlot {
				id: SourceId::new(2),
				current_owner: None,
				latest_request: SourceRequestId::FIRST,
			},
		);
		let mut builder = BaselineBuilder::new();
		host.contribute(&mut builder).expect("source contribution");
		let baseline = builder.finish().expect("baseline");
		assert_eq!(
			baseline
				.sources
				.iter()
				.map(|entry| (entry.source, entry.owner))
				.collect::<Vec<_>>(),
			[
				(SourceId::new(1), RegistryOwnerId::new(10)),
				(SourceId::new(3), RegistryOwnerId::new(30)),
			]
		);
	}

	#[test]
	fn source_limit_and_utf8_are_checked_before_compile() {
		let path = temp_path("limits");
		fs::write(&path, vec![b'x'; MAX_SOURCE_BYTES + 1]).unwrap();
		assert!(read_source(&path).unwrap_err().contains("exceeds"));
		fs::write(&path, [0xff]).unwrap();
		assert!(read_source(&path).unwrap_err().contains("UTF-8"));
		let _ = fs::remove_file(path);
	}

	#[test]
	fn absent_source_is_a_clean_noop() {
		assert_eq!(read_source(&temp_path("missing")).unwrap(), None);
	}

	#[test]
	fn retained_budget_accepts_exact_reload_bounds_and_rejects_steady_overflow() {
		let mut budget = RetainedRuneBudget::new();
		budget.charged = SESSION_RETAINED_CAP;
		assert!(
			budget
				.admit_generation(MAX_RETAINED_PER_GENERATION, 0, MAX_RETAINED_PER_GENERATION,)
				.is_ok(),
			"equal-sized replacement at both exact bounds is admitted"
		);
		assert!(
			budget
				.admit_generation(
					MAX_RETAINED_PER_GENERATION,
					0,
					MAX_RETAINED_PER_GENERATION - 1,
				)
				.is_err(),
			"the first steady-state byte beyond the session cap is rejected"
		);
		assert!(budget.admit_generation(1, 0, 0).is_err());
		assert!(
			budget
				.admit_generation(MAX_RETAINED_PER_GENERATION, 1, SESSION_RETAINED_CAP)
				.is_err(),
			"the per-generation ceiling is independently enforced"
		);
	}

	#[test]
	fn compile_reservation_blocks_double_admission_and_shrinks_to_unit_charge() {
		let mut budget = RetainedRuneBudget::new();
		let reservation = budget.reserve_compile().unwrap();
		assert!(budget.reserve_compile().is_err());
		budget.retain_compiled(reservation, 8 * 1024).unwrap();
		assert_eq!(budget.reserved, 8 * 1024);
		assert!(budget.evaluation_allowance(8 * 1024).unwrap() > 0);
		budget.release_reservation();
		assert_eq!(budget.reserved, 0);
	}

	#[test]
	fn retained_deltas_are_signed_and_checked() {
		assert_eq!(apply_signed_bytes(1_000, 250).unwrap(), 1_250);
		assert_eq!(apply_signed_bytes(1_000, -750).unwrap(), 250);
		assert!(apply_signed_bytes(100, -101).is_err());
	}

	#[test]
	fn foreground_status_is_utf8_safe_and_hard_bounded() {
		let message = "é".repeat(super::MAX_STATUS_BYTES);
		let bounded = bounded_status(&message);
		assert!(bounded.len() <= super::MAX_STATUS_BYTES);
		assert!(bounded.ends_with('…'));
	}

	#[test]
	fn source_identity_caps_accept_exact_boundaries_and_reject_next() {
		assert!(
			validate_source_key_admission(
				MAX_SOURCE_SLOTS - 1,
				MAX_SOURCE_KEY_TOTAL_BYTES - MAX_SOURCE_KEY_BYTES,
				MAX_SOURCE_KEY_BYTES,
			)
			.is_ok()
		);
		assert!(
			validate_source_key_admission(
				MAX_SOURCE_SLOTS,
				MAX_SOURCE_KEY_TOTAL_BYTES - MAX_SOURCE_KEY_BYTES,
				MAX_SOURCE_KEY_BYTES,
			)
			.is_err()
		);
		assert!(validate_source_key_admission(0, 0, MAX_SOURCE_KEY_BYTES + 1).is_err());
		assert!(
			validate_source_key_admission(
				0,
				MAX_SOURCE_KEY_TOTAL_BYTES - MAX_SOURCE_KEY_BYTES + 1,
				MAX_SOURCE_KEY_BYTES,
			)
			.is_err()
		);
		assert!(
			validate_source_path_input(&PathBuf::from("x".repeat(MAX_SOURCE_KEY_BYTES))).is_ok()
		);
		assert!(
			validate_source_path_input(&PathBuf::from("x".repeat(MAX_SOURCE_KEY_BYTES + 1)))
				.is_err()
		);
	}

	#[test]
	fn source_ids_are_monotonic_never_reused_and_exhaustion_checked() {
		let configured_init = temp_path("source-id-init");
		let (_, _, internal, _) = crate::engine::request::channel();
		let services = Arc::new(Mutex::new(HostServices::default()));
		let mut host = RuneHost::new(RuneHostConfig::new(configured_init), services, internal)
			.expect("construct Rune host");
		let first = host.allocate_source_id().unwrap();
		let second = host.allocate_source_id().unwrap();
		assert_eq!(first, crate::core::SourceId::new(1));
		assert_eq!(second, crate::core::SourceId::new(2));
		host.next_source_id = crate::core::SourceId::new(u64::MAX);
		assert!(host.allocate_source_id().is_err());
	}

	#[test]
	fn source_scheduler_coalesces_per_key_without_conflating_distinct_sources() {
		let configured_init = temp_path("scheduler-init");
		let first = temp_path("scheduler-first");
		let second = temp_path("scheduler-second");
		let (_, _, internal, _) = crate::engine::request::channel();
		let services = Arc::new(Mutex::new(HostServices::default()));
		let mut host = RuneHost::new(RuneHostConfig::new(configured_init), services, internal)
			.expect("construct Rune host");
		host.pending_compile = Some(PendingCompile {
			key: SourceKey::Init,
			request: SourceRequestId(999),
			reservation: 1,
		});

		host.request_source(first.clone()).unwrap();
		let first_key = host.normalize_source(first.clone()).unwrap().0;
		let superseded = host.sources[&first_key].latest_request;
		host.request_source(first).unwrap();
		let current = host.sources[&first_key].latest_request;
		assert!(current > superseded);
		assert_eq!(host.queued_sources[&first_key].request, current);
		assert_eq!(
			host.queued_order
				.iter()
				.filter(|key| *key == &first_key)
				.count(),
			1,
			"same-key work keeps one queue position"
		);

		host.request_source(second.clone()).unwrap();
		let second_key = host.normalize_source(second).unwrap().0;
		assert_ne!(first_key, second_key);
		assert!(host.queued_sources.contains_key(&first_key));
		assert!(host.queued_sources.contains_key(&second_key));
		assert_eq!(host.queued_order.len(), 2);
	}

	#[test]
	fn source_publication_reload_and_unload_emit_stable_ordered_identity() {
		let configured_init = temp_path("event-init");
		let source_path = temp_path("event-source");
		let (_, _, internal, _) = crate::engine::request::channel();
		let services = Arc::new(Mutex::new(HostServices::default()));
		let mut host = RuneHost::new(
			RuneHostConfig::new(configured_init),
			services.clone(),
			internal,
		)
		.expect("construct Rune host");
		let registry = Arc::new(UastRegistry::new(1_024));
		let (notify, _notifications) = std::sync::mpsc::channel();
		let resolver = Arc::new(crate::svp::SvpResolver::new_blocking_for_tests(
			registry.clone(),
			notify,
		));
		let session = EditorSession::new(
			&registry,
			"onedark".to_string(),
			false,
			false,
			true,
			false,
			true,
			true,
			SaveDurability::default(),
			UiColorOverrides::default(),
			BTreeMap::new(),
			None,
		);
		let mut editor = EditorHost::new(session, services, registry, resolver);
		let (key, _) = host.normalize_source(source_path.clone()).unwrap();
		let source = host.allocate_source_id().unwrap();
		let request = host.allocate_source_request().unwrap();
		host.sources.insert(
			key.clone(),
			SourceSlot {
				id: source,
				current_owner: None,
				latest_request: request,
			},
		);

		let publish = |host: &mut RuneHost, editor: &mut EditorHost| {
			let reservation = host.retained.reserve_compile().unwrap();
			let compiled = rune::alloc::limit::with(reservation, || {
				compile_source(
					host.context.as_ref().expect("build context"),
					std::path::Path::new("event.rn"),
					"pub fn main(editor) { let _ = editor; }",
				)
			})
			.call()
			.unwrap();
			host.retained
				.retain_compiled(reservation, compiled.retained_bytes)
				.unwrap();
			host.publish_compiled_inner(
				key.clone(),
				compiled,
				editor,
				super::STARTUP_INSTRUCTION_BUDGET,
			)
			.unwrap();
		};

		publish(&mut host, &mut editor);
		let first_owner = host.sources[&key].current_owner.unwrap();
		host.autocmd_failures.insert(
			first_owner,
			[(crate::core::SubscriptionId::new(99), 1)]
				.into_iter()
				.collect(),
		);
		assert_eq!(
			editor
				.session
				.events
				.take_pass()
				.into_iter()
				.collect::<Vec<_>>(),
			vec![EngineEvent::SourceLoaded {
				source,
				owner: first_owner,
			}]
		);

		publish(&mut host, &mut editor);
		let second_owner = host.sources[&key].current_owner.unwrap();
		assert_ne!(first_owner, second_owner);
		assert!(!host.autocmd_failures.contains_key(&first_owner));
		assert_eq!(
			editor
				.session
				.events
				.take_pass()
				.into_iter()
				.collect::<Vec<_>>(),
			vec![
				EngineEvent::SourceUnloaded {
					source,
					owner: first_owner,
				},
				EngineEvent::SourceLoaded {
					source,
					owner: second_owner,
				},
			]
		);

		host.autocmd_failures.insert(
			second_owner,
			[(crate::core::SubscriptionId::new(100), 1)]
				.into_iter()
				.collect(),
		);
		host.unload_source(source_path, &mut editor).unwrap();
		assert!(!host.autocmd_failures.contains_key(&second_owner));
		assert_eq!(
			editor
				.session
				.events
				.take_pass()
				.into_iter()
				.collect::<Vec<_>>(),
			vec![EngineEvent::SourceUnloaded {
				source,
				owner: second_owner,
			}]
		);
	}

	#[test]
	fn rune_autocmd_failures_mute_at_the_exact_boundary() {
		let configured_init = temp_path("autocmd-failure-init");
		let (_, _, internal, _) = crate::engine::request::channel();
		let services = Arc::new(Mutex::new(HostServices::default()));
		let mut host = RuneHost::new(
			RuneHostConfig::new(configured_init),
			services.clone(),
			internal,
		)
		.expect("construct Rune host");
		let registry = Arc::new(UastRegistry::new(1_024));
		let (notify, _notifications) = std::sync::mpsc::channel();
		let resolver = Arc::new(crate::svp::SvpResolver::new_blocking_for_tests(
			registry.clone(),
			notify,
		));
		let session = EditorSession::new(
			&registry,
			"onedark".to_string(),
			false,
			false,
			true,
			false,
			true,
			true,
			SaveDurability::default(),
			UiColorOverrides::default(),
			BTreeMap::new(),
			None,
		);
		let mut editor = EditorHost::new(session, services, registry, resolver);
		let request = host.allocate_source_request().unwrap();
		let source = host.allocate_source_id().unwrap();
		host.sources.insert(
			SourceKey::Init,
			SourceSlot {
				id: source,
				current_owner: None,
				latest_request: request,
			},
		);
		let reservation = host.retained.reserve_compile().unwrap();
		let compiled = rune::alloc::limit::with(reservation, || {
			compile_source(
				host.context.as_ref().expect("build context"),
				std::path::Path::new("autocmd-failure.rn"),
				r#"
pub fn main(editor) {
    let buffer = match baryon::focused_buffer(editor) {
        Ok(value) => value,
        Err(_) => panic("focused buffer"),
    };
    baryon::autocmd_for_buffer(
        editor,
        buffer,
        [baryon::EventKind::BufferEdited],
        |editor, baseline| {},
        |editor, event| {
            if event.inserted_bytes() != Some(0) {
                panic("autocmd boom");
            }
        },
    );
}
"#,
			)
		})
		.call()
		.unwrap();
		host.retained
			.retain_compiled(reservation, compiled.retained_bytes)
			.unwrap();
		host.publish_compiled_inner(
			SourceKey::Init,
			compiled,
			&mut editor,
			super::STARTUP_INSTRUCTION_BUDGET,
		)
		.unwrap();
		editor.session.events.take_pass();

		let mut live = Vec::new();
		let mut needs = Vec::new();
		editor
			.session
			.runtime_registries
			.snapshot_subscriptions(&mut live, &mut needs);
		assert_eq!(needs.len(), 1);
		let id = needs[0];
		let invocation = editor
			.session
			.runtime_registries
			.baseline_subscription_invocation(id)
			.unwrap();
		let mut builder = BaselineBuilder::new();
		editor
			.session
			.contribute_subscriber_baseline(&mut builder)
			.unwrap();
		host.contribute(&mut builder).unwrap();
		let baseline = Arc::new(builder.finish().unwrap());
		assert_eq!(
			host.initialize(&mut editor, invocation, &baseline).result,
			SubscriberResult::Delivered
		);
		assert_eq!(
			Arc::strong_count(&baseline),
			1,
			"baseline wrapper retained its Arc"
		);
		assert!(editor.session.runtime_registries.mark_subscription_live(id));

		let view = editor
			.session
			.layout
			.find_view(crate::core::ViewId::new(1))
			.unwrap();
		let event = EngineEvent::BufferEdited {
			buffer: view.buffer_id,
			acting_view: view.id,
			old_state: crate::core::StateId::new(1),
			new_state: crate::core::StateId::new(2),
			delta_count: 1,
			deleted_bytes: 0,
			inserted_bytes: 1,
		};
		for _ in 0..2 {
			let invocation = editor
				.session
				.runtime_registries
				.live_subscription_invocation(id)
				.unwrap();
			assert_eq!(
				host.deliver(&mut editor, invocation, &event).result,
				SubscriberResult::Failed
			);
		}
		let owner = host.sources[&SourceKey::Init].current_owner.unwrap();
		assert_eq!(host.autocmd_failures[&owner][&id], 2);
		editor
			.session
			.runtime_registries
			.demote_live_subscriptions();
		let invocation = editor
			.session
			.runtime_registries
			.baseline_subscription_invocation(id)
			.unwrap();
		assert_eq!(
			host.initialize(&mut editor, invocation, &baseline).result,
			SubscriberResult::Delivered
		);
		assert!(editor.session.runtime_registries.mark_subscription_live(id));
		assert_eq!(host.autocmd_failures[&owner][&id], 2);
		let mut filtered_event = event.clone();
		let EngineEvent::BufferEdited { buffer, .. } = &mut filtered_event else {
			unreachable!()
		};
		*buffer = crate::core::BufferId::new(u64::MAX);
		let invocation = editor
			.session
			.runtime_registries
			.live_subscription_invocation(id)
			.unwrap();
		assert_eq!(
			host.deliver(&mut editor, invocation, &filtered_event)
				.result,
			SubscriberResult::Delivered
		);
		assert_eq!(host.autocmd_failures[&owner][&id], 2);
		let mut success_event = event.clone();
		let EngineEvent::BufferEdited { inserted_bytes, .. } = &mut success_event else {
			unreachable!()
		};
		*inserted_bytes = 0;
		let invocation = editor
			.session
			.runtime_registries
			.live_subscription_invocation(id)
			.unwrap();
		assert_eq!(
			host.deliver(&mut editor, invocation, &success_event).result,
			SubscriberResult::Delivered
		);
		assert!(!host.autocmd_failures.contains_key(&owner));

		for failure in 1..=super::MAX_AUTOCMD_CONSECUTIVE_FAILURES {
			let invocation = editor
				.session
				.runtime_registries
				.live_subscription_invocation(id)
				.unwrap();
			let outcome = host.deliver(&mut editor, invocation, &event);
			assert_eq!(outcome.result, SubscriberResult::Failed);
			assert_eq!(
				outcome.foreground_status.is_some(),
				failure == 1 || failure == super::MAX_AUTOCMD_CONSECUTIVE_FAILURES
			);
		}
		assert_eq!(host.autocmd_failures[&owner][&id], 8);
		let invocation = editor
			.session
			.runtime_registries
			.live_subscription_invocation(id)
			.unwrap();
		let muted = host.deliver(&mut editor, invocation, &event);
		assert_eq!(muted.result, SubscriberResult::Delivered);
		assert_eq!(host.autocmd_failures[&owner][&id], 8);
		assert_eq!(event.kind(), Some(EventKind::BufferEdited));
		assert!(host.retained.charged > 0);
		let init_path = host.config.init_script.clone();
		host.unload_source(init_path, &mut editor).unwrap();
		assert_eq!(host.retained.charged, 0);
		assert!(!host.autocmd_failures.contains_key(&owner));
	}

	#[test]
	fn rune_autocmd_rejects_nested_callback_owners_that_do_not_match_the_lease() {
		let (_, _, internal, _) = crate::engine::request::channel();
		let services = Arc::new(Mutex::new(HostServices::default()));
		let mut host = RuneHost::new(
			RuneHostConfig::new(temp_path("autocmd-owner-init")),
			services.clone(),
			internal,
		)
		.unwrap();
		let registry = Arc::new(UastRegistry::new(1_024));
		let (notify, _notifications) = std::sync::mpsc::channel();
		let resolver = Arc::new(crate::svp::SvpResolver::new_blocking_for_tests(
			registry.clone(),
			notify,
		));
		let session = EditorSession::new(
			&registry,
			"onedark".to_string(),
			false,
			false,
			true,
			false,
			true,
			true,
			SaveDurability::default(),
			UiColorOverrides::default(),
			BTreeMap::new(),
			None,
		);
		let mut editor = EditorHost::new(session, services, registry, resolver);
		let owner = editor.session.runtime_registries.allocate_owner().unwrap();
		let mut batch = RegistrationBatch::new(owner);
		batch.subscriptions.push(RuntimeSubscriptionRegistration {
			kinds: EventKindSet::one(EventKind::BufferEdited),
			action: ActionLease::new(
				owner,
				RuneAutocmdRef {
					initialize: RuneCallbackRef {
						owner: owner.next(),
						callback_id: 0,
					},
					event: RuneCallbackRef {
						owner,
						callback_id: 1,
					},
					buffer_filter: None,
				},
			),
		});
		editor
			.session
			.runtime_registries
			.commit_owner_batch(batch)
			.unwrap();
		let mut live = Vec::new();
		let mut needs = Vec::new();
		editor
			.session
			.runtime_registries
			.snapshot_subscriptions(&mut live, &mut needs);
		let invocation = editor
			.session
			.runtime_registries
			.baseline_subscription_invocation(needs[0])
			.unwrap();
		let baseline = Arc::new(SubscriberBaseline {
			buffers: Vec::new(),
			views: Vec::new(),
			focused: None,
			sources: Vec::new(),
		});
		let outcome = host.initialize(&mut editor, invocation, &baseline);
		assert_eq!(outcome.result, SubscriberResult::Failed);
		assert!(
			outcome
				.foreground_status
				.expect("capability error status")
				.into_string()
				.contains("owner does not match")
		);
	}

	#[test]
	fn script_loop_and_host_spam_hit_distinct_limits() {
		let services = Arc::new(Mutex::new(HostServices::default()));
		let (context, runtime) = build_context(services.clone()).unwrap();
		let compiled = rune::alloc::limit::with(1024 * 1024, || {
			compile_source(
				&context,
				std::path::Path::new("loop.rn"),
				"pub fn main() { while true {} }",
			)
		})
		.call()
		.unwrap();
		let mut vm = Vm::new(Arc::new(runtime), Arc::new(compiled.unit));
		let result = budget::with(100, || vm.call(["main"], ())).call();
		assert!(
			result.is_err(),
			"instruction budget must stop a script loop"
		);

		lock_services(&services).budget.reset(3);
		for _ in 0..3 {
			assert!(write_message(&services, "x", true).is_ok());
		}
		assert!(
			write_message(&services, "quota", true).is_err(),
			"host-call quota is independent of the VM instruction budget"
		);
		lock_services(&services).budget.reset(1);
		assert!(
			write_message(
				&services,
				&"x".repeat(super::MAX_HOST_OUTPUT_CALL_BYTES + 1),
				true,
			)
			.is_err(),
			"native output rejects oversized work before scanning it"
		);
		assert!(lock_services(&services).messages.total_bytes() <= 256 * 1024);
	}

	#[test]
	fn startup_result_errors_are_not_discarded() {
		let services = Arc::new(Mutex::new(HostServices::default()));
		let (context, runtime) = build_context(services).unwrap();
		let compiled = rune::alloc::limit::with(1024 * 1024, || {
			compile_source(
				&context,
				std::path::Path::new("startup-error.rn"),
				"pub fn main() { Err(\"invalid startup setting\") }",
			)
		})
		.call()
		.unwrap();
		let mut vm = Vm::new(Arc::new(runtime), Arc::new(compiled.unit));
		let value = vm.call(["main"], ()).unwrap();
		assert_eq!(
			super::decode_startup_return(value),
			Err("invalid startup setting".to_string())
		);
	}

	#[test]
	fn guarded_editor_escapes_are_invalidated_without_aborting() {
		const TEST_NAME: &str = "engine::rune_host::lifecycle::tests::guarded_editor_escapes_are_invalidated_without_aborting";
		if let Ok(case) = std::env::var("BARYON_RUNE_ESCAPE_CASE") {
			run_escape_case(&case);
			return;
		}
		for case in ["direct", "vec", "object", "tuple", "closure"] {
			let output = std::process::Command::new(std::env::current_exe().unwrap())
				.arg("--exact")
				.arg(TEST_NAME)
				.arg("--nocapture")
				.env("BARYON_RUNE_ESCAPE_CASE", case)
				.output()
				.unwrap();
			assert!(
				output.status.success(),
				"guarded escape case {case} aborted or failed:\n{}\n{}",
				String::from_utf8_lossy(&output.stdout),
				String::from_utf8_lossy(&output.stderr)
			);
		}
	}

	#[test]
	fn guarded_autocmd_snapshot_escapes_are_invalidated_without_aborting() {
		const TEST_NAME: &str = "engine::rune_host::lifecycle::tests::guarded_autocmd_snapshot_escapes_are_invalidated_without_aborting";
		if let Ok(case) = std::env::var("BARYON_RUNE_AUTOCMD_ESCAPE_CASE") {
			run_autocmd_escape_case(&case);
			return;
		}
		for case in [
			"event-direct",
			"event-vec",
			"baseline-direct",
			"baseline-vec",
		] {
			let output = std::process::Command::new(std::env::current_exe().unwrap())
				.arg("--exact")
				.arg(TEST_NAME)
				.arg("--nocapture")
				.env("BARYON_RUNE_AUTOCMD_ESCAPE_CASE", case)
				.output()
				.unwrap();
			assert!(
				output.status.success(),
				"guarded autocmd escape case {case} aborted or failed:\n{}\n{}",
				String::from_utf8_lossy(&output.stdout),
				String::from_utf8_lossy(&output.stderr)
			);
		}
	}

	fn run_autocmd_escape_case(case: &str) {
		let source = if case.ends_with("direct") {
			"pub fn escape(value) { value }"
		} else {
			"pub fn escape(value) { [value] }"
		};
		let services = Arc::new(Mutex::new(HostServices::default()));
		let (context, runtime) = build_context(services).unwrap();
		let compiled = rune::alloc::limit::with(4 * 1024 * 1024, || {
			compile_source(&context, std::path::Path::new("autocmd-escape.rn"), source)
		})
		.call()
		.unwrap();
		let mut vm = Vm::new(Arc::new(runtime), Arc::new(compiled.unit));
		let value = if case.starts_with("event") {
			let snapshot = RuneAutocmdEvent::new(&EngineEvent::HydrationCompleted {
				buffer: crate::core::BufferId::new(1),
			});
			vm.call(["escape"], (&snapshot,)).expect("escaped event")
		} else {
			let snapshot = RuneAutocmdBaseline::new(Arc::new(SubscriberBaseline {
				buffers: Vec::new(),
				views: Vec::new(),
				focused: None,
				sources: Vec::new(),
			}));
			vm.call(["escape"], (&snapshot,)).expect("escaped baseline")
		};
		let nested = if case.ends_with("direct") {
			value
		} else {
			value
				.downcast::<RuneVec>()
				.unwrap()
				.get(0usize)
				.unwrap()
				.clone()
		};
		if case.starts_with("event") {
			assert!(nested.into_ref::<RuneAutocmdEvent>().is_err());
		} else {
			assert!(nested.into_ref::<RuneAutocmdBaseline>().is_err());
		}
	}

	fn run_escape_case(case: &str) {
		let source = match case {
			"direct" => "pub fn escape(editor) { editor }",
			"vec" => "pub fn escape(editor) { [editor] }",
			"object" => "pub fn escape(editor) { #{value: editor} }",
			"tuple" => "pub fn escape(editor) { (editor,) }",
			"closure" => "pub fn escape(editor) { move || editor }",
			_ => panic!("unknown escape case"),
		};
		let services = Arc::new(Mutex::new(HostServices::default()));
		let (context, runtime) = build_context(services.clone()).unwrap();
		let compiled = rune::alloc::limit::with(4 * 1024 * 1024, || {
			compile_source(&context, std::path::Path::new("escape.rn"), source)
		})
		.call()
		.unwrap();
		let registry = Arc::new(UastRegistry::new(1_024));
		let (notify, _notifications) = std::sync::mpsc::channel();
		let resolver = Arc::new(crate::svp::SvpResolver::new_blocking_for_tests(
			registry.clone(),
			notify,
		));
		let session = EditorSession::new(
			&registry,
			"onedark".to_string(),
			false,
			false,
			true,
			false,
			true,
			true,
			SaveDurability::default(),
			UiColorOverrides::default(),
			BTreeMap::new(),
			None,
		);
		let mut editor = EditorHost::new(session, services, registry, resolver);
		let mut vm = Vm::new(Arc::new(runtime), Arc::new(compiled.unit));
		let result = vm.call(["escape"], (&mut editor,));
		if case == "closure" {
			assert!(
				result.is_err(),
				"capturing the guarded editor must be rejected"
			);
			return;
		}
		let value = result.expect("container may escape only as an invalidated value");
		let nested = match case {
			"direct" => value,
			"vec" => value
				.downcast::<RuneVec>()
				.unwrap()
				.get(0usize)
				.unwrap()
				.clone(),
			"object" => value
				.downcast::<Object>()
				.unwrap()
				.get("value")
				.unwrap()
				.clone(),
			"tuple" => value.into_tuple().unwrap()[0].clone(),
			_ => unreachable!(),
		};
		assert!(
			nested.into_mut::<EditorHost>().is_err(),
			"escaped editor handle must be invalidated"
		);
	}

	// VM-entry scaffold characterization (plans/vm-entry-scaffold.md).
	//
	// These tests pin the shared entry behavior of the three VM-entry paths
	// (dynamic actions, autocmds, job completions) ahead of the scaffold
	// extraction: rejection without consuming the generation, allowance
	// gating, exactly-once signed-delta accounting, per-entry budget/effects
	// reset, deferred-job finalization, and owner bookkeeping.

	struct EntryHarness {
		host: RuneHost,
		editor: EditorHost,
		owner: RegistryOwnerId,
		requests: std::sync::mpsc::Receiver<crate::engine::request::EngineRequest>,
	}

	fn publish_entry_harness(script: &str) -> EntryHarness {
		let configured_init = temp_path("entry-harness-init");
		let (_, _, internal, requests) = crate::engine::request::channel();
		let services = Arc::new(Mutex::new(HostServices::default()));
		let mut host = RuneHost::new(
			RuneHostConfig::new(configured_init),
			services.clone(),
			internal.clone(),
		)
		.expect("construct Rune host");
		let registry = Arc::new(UastRegistry::new(1_024));
		let (notify, _notifications) = std::sync::mpsc::channel();
		let resolver = Arc::new(crate::svp::SvpResolver::new_blocking_for_tests(
			registry.clone(),
			notify,
		));
		let session = EditorSession::new(
			&registry,
			"onedark".to_string(),
			false,
			false,
			true,
			false,
			true,
			true,
			SaveDurability::default(),
			UiColorOverrides::default(),
			BTreeMap::new(),
			None,
		);
		let mut editor = EditorHost::new(session, services, registry, resolver);
		editor.session.jobs.attach_internal_sender(internal);
		editor.set_language_registry(Arc::new(crate::engine::host::build_language_registry()));
		let request = host.allocate_source_request().unwrap();
		let source = host.allocate_source_id().unwrap();
		host.sources.insert(
			SourceKey::Init,
			SourceSlot {
				id: source,
				current_owner: None,
				latest_request: request,
			},
		);
		let reservation = host.retained.reserve_compile().unwrap();
		let compiled = rune::alloc::limit::with(reservation, || {
			compile_source(
				host.context.as_ref().expect("build context"),
				std::path::Path::new("entry-harness.rn"),
				script,
			)
		})
		.call()
		.unwrap();
		host.retained
			.retain_compiled(reservation, compiled.retained_bytes)
			.unwrap();
		host.publish_compiled_inner(
			SourceKey::Init,
			compiled,
			&mut editor,
			super::STARTUP_INSTRUCTION_BUDGET,
		)
		.unwrap();
		editor.session.events.take_pass();
		let owner = host.sources[&SourceKey::Init].current_owner.unwrap();
		EntryHarness {
			host,
			editor,
			owner,
			requests,
		}
	}

	#[test]
	fn init_config_api_applies_builtin_options() {
		let harness = publish_entry_harness(
			r##"
pub fn main(editor) {
			baryon::set(editor, "minimap", true)?;
			editor.config().set("reload", false)?;
    let config = editor.config();
    config.set("wrap", false)?;
    config.set("theme", "gruvbox")?;
			config.ui_color("accent", "#123456")?;
			config.language_theme("markdown", "viridis_cs8")?;
    Ok(())
}
"##,
		);
		assert!(harness.editor.session.persistent_settings.minimap_enabled);
		assert!(!harness.editor.session.persistent_settings.reload_enabled);
		assert!(!harness.editor.session.persistent_settings.wrap_enabled);
		assert_eq!(
			harness.editor.session.persistent_settings.theme_name,
			"gruvbox"
		);
		assert_eq!(
			harness
				.editor
				.session
				.persistent_settings
				.ui_colors
				.get(UiColorKey::Accent)
				.expect("accent override")
				.to_hex(),
			"#123456"
		);
		assert_eq!(
			harness
				.editor
				.session
				.persistent_settings
				.language_themes
				.get("markdown")
				.map(String::as_str),
			Some("viridis_cs8")
		);
	}

	#[test]
	fn rune_wrap_option_resets_display_row_scroll_offset() {
		let mut harness = publish_entry_harness("pub fn main(editor) {}");
		let pane = harness
			.editor
			.session
			.layout
			.find_pane_mut(harness.editor.session.focused_pane_id)
			.expect("focused pane");
		match &mut pane.active_tab_mut().content {
			crate::engine::session::ViewContent::Text(text) => {
				text.state.common.scroll_row_offset = crate::core::DisplayRow::new(7);
			}
			_ => panic!("entry harness should have a text view"),
		}
		super::config_api::apply_option(&mut harness.editor, "wrap", OptionValue::Bool(false))
			.expect("wrap option applies");
		let pane = harness
			.editor
			.session
			.layout
			.find_pane(harness.editor.session.focused_pane_id)
			.expect("focused pane");
		match &pane.active_tab().content {
			crate::engine::session::ViewContent::Text(text) => assert_eq!(
				text.state.common.scroll_row_offset,
				crate::core::DisplayRow::ZERO
			),
			_ => panic!("entry harness should have a text view"),
		}
	}

	#[test]
	fn callback_result_errors_are_decoded_for_all_callback_shapes() {
		let error = rune::to_value(Result::<(), &str>::Err("autocmd failed")).unwrap();
		assert_eq!(
			super::decode_callback_unit(error),
			Err("autocmd failed".to_string())
		);

		let error = rune::to_value(Result::<String, &str>::Err("job failed")).unwrap();
		assert_eq!(
			super::decode_callback_value(error, false),
			Err("job failed".to_string())
		);
	}

	const ENTRY_COMMANDS_SCRIPT: &str = r#"
pub fn main(editor) {
    let cache = [];
    baryon::command(editor, "grow", |editor, args, bang| {
        cache.push(args);
    });
    baryon::command(editor, "shrink", |editor, args, bang| {
        cache.clear();
    });
    baryon::command(editor, "chatty", |editor, args, bang| {
        baryon::test_set_status(editor, "one");
        baryon::test_set_status(editor, "two");
        baryon::test_set_status(editor, "three");
    });
}
"#;

	use crate::engine::runtime_registry::InvocationSnapshot;

	fn command_snapshot(harness: &EntryHarness, name: &str, args: &str) -> InvocationSnapshot {
		harness
			.editor
			.session
			.runtime_registries
			.command_invocation(name, args, false)
			.expect("registered command resolves")
	}

	fn forged_snapshot(
		lease_owner: RegistryOwnerId,
		payload_owner: RegistryOwnerId,
		callback_id: usize,
	) -> InvocationSnapshot {
		InvocationSnapshot::new(
			crate::core::RegistryRev::new(1),
			ActionLease::new(
				lease_owner,
				RuneCallbackRef {
					owner: payload_owner,
					callback_id,
				},
			),
			crate::engine::runtime_registry::DynamicInvocation::Command {
				canonical_name: "forged".to_string(),
				spelling: "forged".to_string(),
				args: String::new(),
				bang: false,
			},
		)
	}

	#[test]
	fn dynamic_entry_rejections_leave_the_generation_and_charge_intact() {
		use crate::engine::runtime_registry::{ActionError, DynamicActionExecutor};
		let mut harness = publish_entry_harness(ENTRY_COMMANDS_SCRIPT);
		let owner = harness.owner;
		let foreign = RegistryOwnerId::new(owner.get() + 1);
		let baseline_charged = harness.host.retained.charged;

		let mismatch = harness
			.host
			.invoke(&mut harness.editor, forged_snapshot(owner, foreign, 0))
			.unwrap_err();
		assert_eq!(
			mismatch,
			ActionError::Failed("Rune callback owner does not match its invocation".to_string())
		);

		let retired = harness
			.host
			.invoke(&mut harness.editor, forged_snapshot(foreign, foreign, 0))
			.unwrap_err();
		assert_eq!(retired, ActionError::RetiredOwner(foreign));

		let invalid = harness
			.host
			.invoke(&mut harness.editor, forged_snapshot(owner, owner, 9_999))
			.unwrap_err();
		assert_eq!(
			invalid,
			ActionError::Failed("Rune callback ID is invalid".to_string())
		);

		assert_eq!(harness.host.retained.charged, baseline_charged);
		assert!(harness.editor.active_callback_owner().is_none());
		// The generation survived every rejection: a real command still runs.
		let snapshot = command_snapshot(&harness, "chatty", "");
		harness
			.host
			.invoke(&mut harness.editor, snapshot)
			.expect("generation must remain invocable after rejections");
	}

	#[test]
	fn dynamic_entry_zero_allowance_rejects_before_vm_entry_and_restores() {
		use crate::engine::runtime_registry::{ActionError, DynamicActionExecutor};
		let mut harness = publish_entry_harness(ENTRY_COMMANDS_SCRIPT);
		let baseline_charged = harness.host.retained.charged;

		harness.host.retained.charged = SESSION_RETAINED_CAP;
		let snapshot = command_snapshot(&harness, "chatty", "");
		let rejected = harness
			.host
			.invoke(&mut harness.editor, snapshot)
			.unwrap_err();
		assert_eq!(
			rejected,
			ActionError::Failed("Rune callback retained-memory limit is exhausted".to_string())
		);

		harness.host.retained.charged = baseline_charged;
		let snapshot = command_snapshot(&harness, "chatty", "");
		harness
			.host
			.invoke(&mut harness.editor, snapshot)
			.expect("generation must be restored after an allowance rejection");
	}

	#[test]
	fn dynamic_entry_applies_signed_deltas_exactly_once() {
		let mut harness = publish_entry_harness(ENTRY_COMMANDS_SCRIPT);
		let baseline_charged = harness.host.retained.charged;

		let payload = "x".repeat(4 * 1024);
		let cycle = |harness: &mut EntryHarness| {
			use crate::engine::runtime_registry::DynamicActionExecutor;
			let snapshot = command_snapshot(harness, "grow", &payload);
			harness
				.host
				.invoke(&mut harness.editor, snapshot)
				.expect("grow command");
			let grown = harness.host.retained.charged;
			let snapshot = command_snapshot(harness, "shrink", "");
			harness
				.host
				.invoke(&mut harness.editor, snapshot)
				.expect("shrink command");
			(grown, harness.host.retained.charged)
		};

		let (first_grown, first_shrunk) = cycle(&mut harness);
		assert!(
			first_grown >= baseline_charged + payload.len(),
			"retaining the argument must charge at least its length"
		);
		// clear() keeps the vector's capacity buffer, so a small residual
		// charge over the baseline is genuine retained memory.
		assert!(
			first_shrunk < baseline_charged + 256,
			"releasing the retained argument must return the charge minus capacity"
		);

		// A second cycle must be byte-identical at both edges: any drift means
		// a delta was applied more or less than exactly once.
		let (second_grown, second_shrunk) = cycle(&mut harness);
		assert_eq!(second_grown, first_grown);
		assert_eq!(second_shrunk, first_shrunk);
	}

	#[test]
	fn dynamic_entry_resets_host_budget_and_owner_bookkeeping_per_entry() {
		use crate::engine::runtime_registry::DynamicActionExecutor;
		let mut harness = publish_entry_harness(ENTRY_COMMANDS_SCRIPT);

		// Exhaust the budget outside the entry; the entry must reset it before
		// the callback runs, so a multi-call callback still succeeds.
		lock_services(&harness.editor.services).budget.reset(0);
		let snapshot = command_snapshot(&harness, "chatty", "");
		harness
			.host
			.invoke(&mut harness.editor, snapshot)
			.expect("entry must reset the host budget before the callback");
		assert!(
			harness.editor.active_callback_owner().is_none(),
			"the active owner must be cleared on every exit"
		);
	}

	const ENTRY_JOBS_SCRIPT: &str = r#"
pub fn main(editor) {
    baryon::command(editor, "kick", |editor, args, bang| {
        let spec = match baryon::JobSpec::new("/bin/echo", ["entry-check"]) {
            Ok(spec) => spec,
            Err(_) => panic("job spec"),
        };
        let _ = match baryon::spawn(editor, spec, |editor, job, result| {
            baryon::test_set_status(editor, "entry-job-done");
        }) {
            Ok(job) => job,
            Err(_) => panic("spawn"),
        };
    });
    baryon::command(editor, "kickfail", |editor, args, bang| {
        let spec = match baryon::JobSpec::new("/bin/echo", ["never"]) {
            Ok(spec) => spec,
            Err(_) => panic("job spec"),
        };
        let spec = match spec.with_stdin(b"reservation-charge") {
            Ok(spec) => spec,
            Err(_) => panic("stdin"),
        };
        let _ = match baryon::spawn(editor, spec, |editor, job, result| {}) {
            Ok(job) => job,
            Err(_) => panic("spawn"),
        };
        panic("post-spawn failure");
    });
}
"#;

	fn recv_job_completed(
		requests: &std::sync::mpsc::Receiver<crate::engine::request::EngineRequest>,
	) -> crate::core::JobId {
		use crate::engine::request::{EngineRequest, InternalEvent};
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		loop {
			let remaining = deadline
				.saturating_duration_since(std::time::Instant::now())
				.max(std::time::Duration::from_millis(1));
			match requests.recv_timeout(remaining) {
				Ok(EngineRequest::Internal(InternalEvent::JobCompleted(id))) => return id,
				Ok(_) => continue,
				Err(error) => panic!("expected JobCompleted: {error}"),
			}
		}
	}

	#[test]
	fn job_completion_entry_commits_deferred_jobs_and_delivers_through_the_vm() {
		use crate::engine::event_subscribers::SubscriberResult;
		use crate::engine::runtime_registry::DynamicActionExecutor;
		let mut harness = publish_entry_harness(ENTRY_JOBS_SCRIPT);

		let snapshot = command_snapshot(&harness, "kick", "");
		harness
			.host
			.invoke(&mut harness.editor, snapshot)
			.expect("kick command");

		let id = recv_job_completed(&harness.requests);
		harness
			.editor
			.session
			.jobs
			.handle_supervisor_report(id, true);
		harness.editor.session.jobs.capture_delivery_batch();
		let (invocation, completion) = harness
			.editor
			.session
			.jobs
			.take_next_delivery()
			.expect("committed job must deliver");
		let outcome = harness
			.host
			.complete_job(&mut harness.editor, invocation, &completion);
		assert_eq!(outcome.result, SubscriberResult::Delivered);
		assert!(harness.editor.active_callback_owner().is_none());
	}

	#[test]
	fn job_completion_entry_discards_reservations_when_the_callback_fails() {
		use crate::engine::runtime_registry::DynamicActionExecutor;
		let mut harness = publish_entry_harness(ENTRY_JOBS_SCRIPT);

		let snapshot = command_snapshot(&harness, "kickfail", "");
		let result = harness.host.invoke(&mut harness.editor, snapshot);
		assert!(result.is_err(), "the callback failure must surface");
		assert_eq!(
			harness.editor.session.jobs.charged_buffer_bytes(),
			0,
			"a VM error must discard the staged reservation and its stdin charge"
		);
		assert!(!harness.editor.session.jobs.has_pending_delivery());
	}

	#[test]
	fn job_completion_entry_silently_consumes_foreign_lease_payloads() {
		use crate::engine::event_subscribers::SubscriberResult;
		let mut harness = publish_entry_harness(ENTRY_JOBS_SCRIPT);
		let owner = harness.owner;
		let foreign = RegistryOwnerId::new(owner.get() + 1);

		let spec = crate::engine::jobs::JobSpec {
			program: PathBuf::from("/bin/echo"),
			args: vec!["foreign".into()],
			cwd: None,
			stdin: None,
			timeout: std::time::Duration::from_secs(5),
			completion: None,
		};
		let reservation = harness
			.editor
			.session
			.jobs
			.reserve_job(owner, true, spec)
			.expect("reserve");
		harness
			.editor
			.session
			.jobs
			.commit_reserved_job(
				reservation,
				ActionLease::new(
					owner,
					RuneCallbackRef {
						owner: foreign,
						callback_id: 0,
					},
				),
			)
			.expect("commit");

		let id = recv_job_completed(&harness.requests);
		harness
			.editor
			.session
			.jobs
			.handle_supervisor_report(id, true);
		harness.editor.session.jobs.capture_delivery_batch();
		let (invocation, completion) = harness
			.editor
			.session
			.jobs
			.take_next_delivery()
			.expect("delivery");
		let baseline_charged = harness.host.retained.charged;
		let outcome = harness
			.host
			.complete_job(&mut harness.editor, invocation, &completion);
		assert_eq!(
			outcome.result,
			SubscriberResult::Delivered,
			"a foreign lease payload is consumed silently, never dispatched"
		);
		assert!(outcome.foreground_status.is_none());
		assert_eq!(harness.host.retained.charged, baseline_charged);
	}

	#[test]
	fn autocmd_entry_allowance_gating_matches_the_dynamic_path() {
		use crate::engine::event_subscribers::SubscriberResult;
		let mut harness = publish_entry_harness(
			r#"
pub fn main(editor) {
    let buffer = match baryon::focused_buffer(editor) {
        Ok(value) => value,
        Err(_) => panic("focused buffer"),
    };
    baryon::autocmd_for_buffer(
        editor,
        buffer,
        [baryon::EventKind::BufferEdited],
        |editor, baseline| {},
        |editor, event| {},
    );
}
"#,
		);

		let mut live = Vec::new();
		let mut needs = Vec::new();
		harness
			.editor
			.session
			.runtime_registries
			.snapshot_subscriptions(&mut live, &mut needs);
		assert_eq!(needs.len(), 1);
		let id = needs[0];
		let mut builder = BaselineBuilder::new();
		harness
			.editor
			.session
			.contribute_subscriber_baseline(&mut builder)
			.unwrap();
		harness.host.contribute(&mut builder).unwrap();
		let baseline = Arc::new(builder.finish().unwrap());

		let baseline_charged = harness.host.retained.charged;
		harness.host.retained.charged = SESSION_RETAINED_CAP;
		let invocation = harness
			.editor
			.session
			.runtime_registries
			.baseline_subscription_invocation(id)
			.unwrap();
		let rejected = harness
			.host
			.initialize(&mut harness.editor, invocation, &baseline);
		assert_eq!(rejected.result, SubscriberResult::Failed);

		harness.host.retained.charged = baseline_charged;
		let invocation = harness
			.editor
			.session
			.runtime_registries
			.baseline_subscription_invocation(id)
			.unwrap();
		let delivered = harness
			.host
			.initialize(&mut harness.editor, invocation, &baseline);
		assert_eq!(
			delivered.result,
			SubscriberResult::Delivered,
			"generation must be restored after an allowance rejection"
		);
	}
}
