use std::collections::{BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use tracing::Instrument;

use crate::distributed::budget::DurationMillis;
use crate::distributed::identity::{AttemptId, QueryId, StageId, TaskIdentity, WorkerId};
use crate::distributed::task::{
	LiveProducerPartitionRef, PartitionAddress, PartitionManifest, ProducerPartitionRef,
	RetryPolicy, ShufflePartitionRef, ShufflePublishMode, TaskCompletionOutput, TaskInputBinding,
	TaskOutputBinding, TaskSpec, TaskTelemetry, exchange_producer_plan_supports_live,
	is_streamable_exchange_producer, is_streamable_shuffle_consumer,
};
use crate::distributed::task_state::{
	AcceptanceState, AttemptState, CancelledTaskCount, InvalidatedTask, TaskKey, TaskStateStore,
};
use crate::distributed::worker_registry::{
	PlacementHints, UnixMillis, WorkerEntry, WorkerRegistry,
};
use crate::error::TaskFailureKind;
use crate::exec::runtime_filter::{
	build_runtime_filter_fragment, descriptor_from_fragment, merge_fragments,
};
#[cfg(test)]
use crate::ir::{BroadcastReplayEligibility, JoinReplayId};
use crate::ir::{ExecutionPlan, JoinStrategy};
use wendelon::schema::PhysicalDescriptorFingerprint;

#[derive(Debug, Clone)]
pub struct StagePlan {
	pub stage_id: StageId,
	pub upstream_stages: Vec<StageId>,
	pub tasks: Vec<TaskPlan>,
}

#[derive(Debug, Clone)]
pub struct TaskPlan {
	pub spec: TaskSpec,
	pub retry_policy: RetryPolicy,
}

impl TaskPlan {
	pub fn new(spec: TaskSpec) -> Self {
		Self {
			spec,
			retry_policy: RetryPolicy::default(),
		}
	}
}

#[async_trait]
pub trait TaskDispatcher {
	async fn dispatch(&self, worker: &WorkerEntry, spec: &TaskSpec) -> DispatchOutcome;
}

#[derive(Debug, Clone)]
pub enum DispatchOutcome {
	Completed(TaskCompletionOutput),
	CompletedWithBatches {
		output: TaskCompletionOutput,
		batches: Vec<RecordBatch>,
	},
	CompletedWithTelemetry {
		output: TaskCompletionOutput,
		batches: Vec<RecordBatch>,
		telemetry: TaskTelemetry,
	},
	Failed(TaskFailureKind),
	FailedWithTelemetry {
		kind: TaskFailureKind,
		telemetry: TaskTelemetry,
	},
}

#[derive(Debug)]
pub struct SchedulerRun {
	pub dispatched: Vec<TaskIdentity>,
	pub accepted_manifests: HashMap<TaskKey, PartitionManifest>,
	pub final_results: Vec<RecordBatch>,
	pub telemetry: SchedulerTelemetry,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchedulerTelemetry {
	pub task_telemetry: Vec<TaskTelemetry>,
	pub retry_count: u64,
	pub stale_rejection_count: u64,
	pub cancellation_cleanup_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerLossInvalidation {
	pub producers: Vec<InvalidatedTask>,
	pub consumers: Vec<InvalidatedTask>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerError {
	NoLiveWorker,
	TaskFailed { key: TaskKey, kind: TaskFailureKind },
	NoProgress { blocked_stages: Vec<StageId> },
	PartitionManifestMismatch { message: String },
	Acceptance(String),
}

struct TaskAttemptCompletion {
	key: TaskKey,
	identity: TaskIdentity,
	worker_id: WorkerId,
	spec: TaskSpec,
	dispatch_wait_micros: u64,
	outcome: DispatchOutcome,
}

type TaskAttemptFuture<'a> = Pin<Box<dyn Future<Output = TaskAttemptCompletion> + Send + 'a>>;

pub struct DistributedScheduler<Registry> {
	registry: Registry,
	state: TaskStateStore,
	accepted_manifests: HashMap<TaskKey, PartitionManifest>,
	task_dependencies: HashMap<TaskKey, Vec<PartitionAddress>>,
	runtime_filter_merges: HashMap<(QueryId, StageId), RuntimeFilterMerge>,
	telemetry: SchedulerTelemetry,
	max_in_flight_tasks: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RuntimeFilterMerge {
	Active(crate::distributed::task::RuntimeFilterFragment),
	Dropped,
}

impl<Registry> DistributedScheduler<Registry>
where
	Registry: WorkerRegistry,
{
	pub fn new(registry: Registry) -> Self {
		Self {
			registry,
			state: TaskStateStore::new(),
			accepted_manifests: HashMap::new(),
			task_dependencies: HashMap::new(),
			runtime_filter_merges: HashMap::new(),
			telemetry: SchedulerTelemetry::default(),
			max_in_flight_tasks: None,
		}
	}

	pub fn with_max_in_flight_tasks(mut self, max_in_flight_tasks: Option<usize>) -> Self {
		self.max_in_flight_tasks = max_in_flight_tasks.map(|limit| limit.max(1));
		self
	}

	pub fn state(&self) -> &TaskStateStore {
		&self.state
	}

	pub fn accepted_manifest(&self, key: TaskKey) -> Option<&PartitionManifest> {
		self.accepted_manifests.get(&key)
	}

	pub fn mark_worker_lost(&mut self, worker_id: WorkerId) -> WorkerLossInvalidation {
		self.registry.mark_lost(worker_id);
		self.invalidate_lost_worker(worker_id)
	}

	pub fn expire_worker_heartbeats(
		&mut self,
		now: UnixMillis,
		suspect_after: DurationMillis,
		lost_after: DurationMillis,
	) -> Vec<WorkerLossInvalidation> {
		self.registry
			.expire_heartbeats(now, suspect_after, lost_after)
			.into_iter()
			.map(|worker_id| self.invalidate_lost_worker(worker_id))
			.collect()
	}

	fn invalidate_lost_worker(&mut self, worker_id: WorkerId) -> WorkerLossInvalidation {
		let producers = self.state.invalidate_worker(worker_id);
		for task in &producers {
			self.accepted_manifests.remove(&task.key);
		}
		let consumers = self.invalidate_consumers_for_producers(&producers);
		WorkerLossInvalidation {
			producers,
			consumers,
		}
	}

	pub fn cancel_query(&mut self, query_id: QueryId) -> CancelledTaskCount {
		self.accepted_manifests
			.retain(|key, _| key.query_id != query_id);
		self.task_dependencies
			.retain(|key, _| key.query_id != query_id);
		self.runtime_filter_merges
			.retain(|(merge_query_id, _), _| *merge_query_id != query_id);
		let cancelled = self.state.cancel_query(query_id);
		self.telemetry.cancellation_cleanup_count = self
			.telemetry
			.cancellation_cleanup_count
			.saturating_add(u64::try_from(cancelled.get()).unwrap_or(u64::MAX));
		cancelled
	}

	pub async fn dispatch_loop<D>(
		&mut self,
		stages: &[StagePlan],
		dispatcher: &D,
	) -> Result<SchedulerRun, SchedulerError>
	where
		D: TaskDispatcher + Send + Sync,
	{
		let mut dispatched = Vec::new();
		let mut final_results = Vec::new();
		let mut completed_tasks = BTreeSet::new();
		let mut in_flight_keys = BTreeSet::new();
		let mut in_flight: FuturesUnordered<TaskAttemptFuture<'_>> = FuturesUnordered::new();
		let mut attempts_spawned = HashMap::<TaskKey, u32>::new();
		let retry_limits = stages
			.iter()
			.flat_map(|stage| {
				stage.tasks.iter().map(|task| {
					(
						TaskKey::from_identity(task.spec.identity),
						task.retry_policy.max_attempts.get(),
					)
				})
			})
			.collect::<HashMap<_, _>>();
		let expected_task_count = stages.iter().map(|stage| stage.tasks.len()).sum::<usize>();

		while completed_tasks.len() < expected_task_count {
			let mut admitted = false;
			let mut blocked_stages = BTreeSet::new();

			'stage_scan: for stage in stages {
				if !self.in_flight_capacity_available(in_flight.len()) {
					break;
				}
				if self.stage_requires_stage_barrier(stage, stages)
					&& !self.stage_barrier_open(&stage.upstream_stages, stages)
				{
					blocked_stages.insert(stage.stage_id);
					continue;
				}

				for task in &stage.tasks {
					if !self.in_flight_capacity_available(in_flight.len()) {
						break 'stage_scan;
					}
					let key = TaskKey::from_identity(task.spec.identity);
					if completed_tasks.contains(&key) || in_flight_keys.contains(&key) {
						continue;
					}
					match self.state.acceptance_state(key) {
						AcceptanceState::Open => {}
						AcceptanceState::Invalidated { .. } => {
							self.state
								.reopen_invalidated(key)
								.map_err(|err| SchedulerError::Acceptance(format!("{err:?}")))?
						}
						AcceptanceState::LiveAccepted { .. }
							if matches!(
								task.spec.output,
								TaskOutputBinding::ShuffleProducer { .. }
							) =>
						{
							completed_tasks.insert(key);
							continue;
						}
						AcceptanceState::LiveAccepted { .. }
						| AcceptanceState::Failed { .. }
						| AcceptanceState::Cancelled => continue,
					}

					if !self.task_ready_for_dispatch(stage, task, stages) {
						blocked_stages.insert(stage.stage_id);
						continue;
					}

					let worker_id = self
						.preferred_live_consumer_worker(stage, task, stages)
						.filter(|worker_id| self.registry.resolve(*worker_id).is_some())
						.or_else(|| self.registry.select_for_attempt(&PlacementHints))
						.ok_or(SchedulerError::NoLiveWorker)?;
					let worker = self
						.registry
						.resolve(worker_id)
						.ok_or(SchedulerError::NoLiveWorker)?;
					let attempt_id = self
						.state
						.pending_attempt(key)
						.map(|identity| identity.attempt_id)
						.unwrap_or_else(AttemptId::first);
					let mut spec = self.materialize_task_spec(task.spec.clone(), stages)?;
					spec.identity = TaskIdentity::new(
						spec.identity.query_id,
						spec.identity.stage_id,
						spec.identity.task_id,
						attempt_id,
					);
					self.state.spawn_attempt(spec.identity);
					self.state
						.set_attempt_state(spec.identity, AttemptState::Dispatched { worker_id });
					self.state
						.set_attempt_state(spec.identity, AttemptState::Running { worker_id });
					dispatched.push(spec.identity);
					in_flight_keys.insert(key);
					*attempts_spawned.entry(key).or_default() += 1;
					admitted = true;

					let dispatch_span = crate::telemetry::dispatch_span(&spec);
					if let Some(trace_context) = crate::telemetry::dispatch_trace_context(&spec) {
						spec.context.trace_context = Some(trace_context);
					}
					in_flight.push(Box::pin(async move {
						let dispatch_started = Instant::now();
						let outcome = dispatcher
							.dispatch(&worker, &spec)
							.instrument(dispatch_span)
							.await;
						TaskAttemptCompletion {
							key,
							identity: spec.identity,
							worker_id,
							spec,
							dispatch_wait_micros: duration_micros(dispatch_started),
							outcome,
						}
					}));
				}
			}

			if completed_tasks.len() >= expected_task_count {
				break;
			}
			if in_flight.is_empty() {
				if admitted {
					continue;
				}
				return Err(SchedulerError::NoProgress {
					blocked_stages: blocked_stages.into_iter().collect(),
				});
			}

			let Some(completion) = in_flight.next().await else {
				continue;
			};
			in_flight_keys.remove(&completion.key);
			self.accept_dispatch_outcome(
				completion,
				stages,
				&mut completed_tasks,
				&attempts_spawned,
				&retry_limits,
				&mut final_results,
			)?;
		}

		Ok(SchedulerRun {
			dispatched,
			accepted_manifests: self.accepted_manifests.clone(),
			final_results,
			telemetry: self.telemetry.clone(),
		})
	}

	fn in_flight_capacity_available(&self, in_flight_count: usize) -> bool {
		match self.max_in_flight_tasks {
			Some(limit) => in_flight_count < limit,
			None => true,
		}
	}

	fn preferred_live_consumer_worker(
		&self,
		stage: &StagePlan,
		task: &TaskPlan,
		stages: &[StagePlan],
	) -> Option<WorkerId> {
		if let Some(worker_id) = self.live_input_worker_for_all_shuffle_inputs(&task.spec, stages) {
			return Some(worker_id);
		}
		if stage.upstream_stages.len() != 1 || !is_streamable_shuffle_consumer(&task.spec) {
			return None;
		}
		let mut shuffle_inputs = task.spec.inputs.iter().filter_map(|input| {
			let TaskInputBinding::ShuffleConsumer {
				producer_stage_id, ..
			} = input
			else {
				return None;
			};
			Some(*producer_stage_id)
		});
		let producer_stage_id = shuffle_inputs.next()?;
		if shuffle_inputs.next().is_some() {
			return None;
		}
		let producers = expected_producer_tasks(stages, producer_stage_id)?;
		if producers.len() != 1 {
			return None;
		}
		let producer = producers.first()?;
		if !is_streamable_exchange_producer(&producer.spec) {
			return None;
		}
		self.state
			.running_attempt(TaskKey::from_identity(producer.spec.identity))
			.map(|(_, worker_id)| worker_id)
	}

	fn accept_dispatch_outcome(
		&mut self,
		completion: TaskAttemptCompletion,
		stages: &[StagePlan],
		completed_tasks: &mut BTreeSet<TaskKey>,
		attempts_spawned: &HashMap<TaskKey, u32>,
		retry_limits: &HashMap<TaskKey, u32>,
		final_results: &mut Vec<RecordBatch>,
	) -> Result<(), SchedulerError> {
		let key = completion.key;
		let spec = completion.spec;
		let dispatch_span = crate::telemetry::dispatch_span(&spec);
		match completion.outcome {
			DispatchOutcome::Completed(output) => {
				self.accept_dispatch_completion(
					&spec,
					stages,
					completion.worker_id,
					key,
					output,
					Vec::new(),
					final_results,
				)?;
				self.refresh_drained_producers(stages);
				completed_tasks.insert(key);
			}
			DispatchOutcome::CompletedWithBatches { output, batches } => {
				self.accept_dispatch_completion(
					&spec,
					stages,
					completion.worker_id,
					key,
					output,
					batches,
					final_results,
				)?;
				self.refresh_drained_producers(stages);
				completed_tasks.insert(key);
			}
			DispatchOutcome::CompletedWithTelemetry {
				output,
				batches,
				mut telemetry,
			} => {
				telemetry.phases.dispatch_wait_micros = telemetry
					.phases
					.dispatch_wait_micros
					.saturating_add(completion.dispatch_wait_micros);
				self.telemetry.task_telemetry.push(telemetry);
				self.accept_dispatch_completion(
					&spec,
					stages,
					completion.worker_id,
					key,
					output,
					batches,
					final_results,
				)?;
				self.refresh_drained_producers(stages);
				completed_tasks.insert(key);
			}
			DispatchOutcome::Failed(kind) => {
				self.accept_dispatch_failure(
					key,
					completion.identity,
					attempts_spawned,
					retry_limits,
					&dispatch_span,
					kind,
				)?;
			}
			DispatchOutcome::FailedWithTelemetry {
				kind,
				mut telemetry,
			} => {
				telemetry.phases.dispatch_wait_micros = telemetry
					.phases
					.dispatch_wait_micros
					.saturating_add(completion.dispatch_wait_micros);
				self.telemetry.task_telemetry.push(telemetry);
				self.accept_dispatch_failure(
					key,
					completion.identity,
					attempts_spawned,
					retry_limits,
					&dispatch_span,
					kind,
				)?;
			}
		}
		Ok(())
	}

	fn accept_dispatch_failure(
		&mut self,
		key: TaskKey,
		identity: TaskIdentity,
		attempts_spawned: &HashMap<TaskKey, u32>,
		retry_limits: &HashMap<TaskKey, u32>,
		dispatch_span: &tracing::Span,
		kind: TaskFailureKind,
	) -> Result<(), SchedulerError> {
		crate::telemetry::record_task_failure(dispatch_span, &kind);
		self.record_dispatch_failure(&kind);
		let retryable = failure_retryable(&kind);
		if retryable
			&& attempts_spawned.get(&key).copied().unwrap_or_default()
				< retry_limits.get(&key).copied().unwrap_or(1)
		{
			self.telemetry.retry_count = self.telemetry.retry_count.saturating_add(1);
			self.state
				.set_attempt_state(identity, AttemptState::FailedRetryable { kind });
			self.state
				.spawn_attempt(key.attempt(identity.attempt_id.next()));
			return Ok(());
		}

		self.state.set_attempt_state(
			identity,
			AttemptState::FailedTerminal { kind: kind.clone() },
		);
		self.state.fail_task(key, kind.clone());
		Err(SchedulerError::TaskFailed { key, kind })
	}

	fn record_dispatch_failure(&mut self, kind: &TaskFailureKind) {
		if task_failure_is_stale_rejection(kind) {
			self.telemetry.stale_rejection_count =
				self.telemetry.stale_rejection_count.saturating_add(1);
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn accept_dispatch_completion(
		&mut self,
		spec: &TaskSpec,
		stages: &[StagePlan],
		worker_id: crate::distributed::identity::WorkerId,
		key: TaskKey,
		output: TaskCompletionOutput,
		result_batches: Vec<RecordBatch>,
		final_results: &mut Vec<RecordBatch>,
	) -> Result<(), SchedulerError> {
		let identity = spec.identity;
		self.state
			.set_attempt_state(identity, AttemptState::Completed { worker_id });
		self.state
			.accept_completed(identity, worker_id)
			.map_err(|err| SchedulerError::Acceptance(format!("{err:?}")))?;
		match output {
			TaskCompletionOutput::FinalResult { .. } => {
				final_results.extend(result_batches);
			}
			TaskCompletionOutput::ShuffleProducer { mut manifest } => {
				self.maybe_attach_runtime_filter_fragment(
					spec,
					stages,
					&result_batches,
					&mut manifest,
				)?;
				self.accepted_manifests.insert(key, manifest);
			}
		}
		let dependencies = task_dependencies(spec);
		if dependencies.is_empty() {
			self.task_dependencies.remove(&key);
		} else {
			self.task_dependencies.insert(key, dependencies);
		}
		Ok(())
	}

	fn invalidate_consumers_for_producers(
		&mut self,
		producers: &[InvalidatedTask],
	) -> Vec<InvalidatedTask> {
		let invalidated_producers = producers
			.iter()
			.map(|producer| (producer.key, producer.invalidated_attempt))
			.collect::<HashSet<_>>();
		if invalidated_producers.is_empty() {
			return Vec::new();
		}

		let consumer_keys = self
			.task_dependencies
			.iter()
			.filter(|(_, dependencies)| {
				dependencies.iter().any(|dependency| {
					invalidated_producers.contains(&(
						TaskKey::new(
							dependency.query_id,
							dependency.producer_stage_id,
							dependency.producer_task_id,
						),
						dependency.attempt_id,
					))
				})
			})
			.map(|(key, _)| *key)
			.collect::<Vec<_>>();

		let mut consumers = Vec::new();
		for key in consumer_keys {
			self.task_dependencies.remove(&key);
			if let Some(invalidated_task) = self.state.invalidate_task(key) {
				consumers.push(invalidated_task);
			}
		}
		consumers
	}

	fn stage_requires_stage_barrier(&self, stage: &StagePlan, stages: &[StagePlan]) -> bool {
		let upstreams_all_streamable_producers = stage
			.upstream_stages
			.iter()
			.all(|upstream| is_live_publishing_producer_stage(*upstream, stages));
		let streamable_both = stage.upstream_stages.len() > 1 && upstreams_all_streamable_producers;
		(stage.upstream_stages.len() != 1 && !streamable_both)
			|| stage.tasks.iter().any(|task| {
				!task
					.spec
					.inputs
					.iter()
					.any(|input| matches!(input, TaskInputBinding::ShuffleConsumer { .. }))
			})
	}

	fn task_ready_for_dispatch(
		&self,
		stage: &StagePlan,
		task: &TaskPlan,
		stages: &[StagePlan],
	) -> bool {
		if stage.upstream_stages.is_empty() {
			return true;
		}
		let upstreams_all_streamable_producers = stage
			.upstream_stages
			.iter()
			.all(|upstream| is_live_publishing_producer_stage(*upstream, stages));
		let streamable_both = stage.upstream_stages.len() > 1 && upstreams_all_streamable_producers;
		let live_eligible = !contains_broadcast_join(&task.spec.plan)
			&& ((stage.upstream_stages.len() == 1 && is_streamable_shuffle_consumer(&task.spec))
				|| streamable_both);
		if !live_eligible {
			return self.shuffle_inputs_fully_sealed(&task.spec, stages);
		}
		if streamable_both {
			return self
				.live_input_worker_for_all_shuffle_inputs(&task.spec, stages)
				.is_some() || self.shuffle_inputs_fully_sealed(&task.spec, stages);
		}

		task.spec.inputs.iter().all(|input| {
			let TaskInputBinding::ShuffleConsumer {
				exchange_id,
				producer_stage_id,
				partition_id,
				..
			} = input
			else {
				return true;
			};
			let Some(producers) = expected_producer_tasks(stages, *producer_stage_id) else {
				return false;
			};
			if producers.len() != 1 {
				return producers.iter().all(|producer| {
					self.sealed_ref_for_producer(&producer.spec, *exchange_id, *partition_id)
						.is_some() || self
						.accepted_manifests
						.contains_key(&TaskKey::from_identity(producer.spec.identity))
				});
			}
			producers.iter().all(|producer| {
				self.sealed_ref_for_producer(&producer.spec, *exchange_id, *partition_id)
					.is_some() || self
					.accepted_manifests
					.contains_key(&TaskKey::from_identity(producer.spec.identity))
					|| self
						.live_ref_for_producer(&producer.spec, *exchange_id, *partition_id)
						.is_some()
			})
		})
	}

	fn shuffle_inputs_fully_sealed(&self, spec: &TaskSpec, stages: &[StagePlan]) -> bool {
		spec.inputs.iter().all(|input| {
			let TaskInputBinding::ShuffleConsumer {
				exchange_id,
				producer_stage_id,
				partition_id,
				..
			} = input
			else {
				return true;
			};
			expected_producer_tasks(stages, *producer_stage_id).is_some_and(|producers| {
				producers.iter().all(|producer| {
					self.sealed_ref_for_producer(&producer.spec, *exchange_id, *partition_id)
						.is_some() || self
						.accepted_manifests
						.contains_key(&TaskKey::from_identity(producer.spec.identity))
				})
			})
		})
	}

	fn live_input_worker_for_all_shuffle_inputs(
		&self,
		spec: &TaskSpec,
		stages: &[StagePlan],
	) -> Option<WorkerId> {
		let mut common_worker_id = None;
		let mut saw_shuffle_input = false;
		for input in &spec.inputs {
			let TaskInputBinding::ShuffleConsumer {
				exchange_id,
				producer_stage_id,
				partition_id,
				..
			} = input
			else {
				continue;
			};
			saw_shuffle_input = true;
			let producers = expected_producer_tasks(stages, *producer_stage_id)?;
			if producers.len() != 1 {
				return None;
			}
			let live =
				self.live_ref_for_producer(&producers[0].spec, *exchange_id, *partition_id)?;
			match common_worker_id {
				Some(worker_id) if worker_id != live.worker_id => return None,
				Some(_) => {}
				None => common_worker_id = Some(live.worker_id),
			}
		}
		if saw_shuffle_input {
			common_worker_id
		} else {
			None
		}
	}

	fn stage_barrier_open(&self, upstream_stages: &[StageId], stages: &[StagePlan]) -> bool {
		if upstream_stages.is_empty() {
			return true;
		}

		upstream_stages.iter().all(|upstream_stage| {
			stages
				.iter()
				.find(|stage| stage.stage_id == *upstream_stage)
				.map(|stage| {
					stage.tasks.iter().all(|task| {
						matches!(
							self.state
								.acceptance_state(TaskKey::from_identity(task.spec.identity)),
							AcceptanceState::LiveAccepted { .. }
						)
					})
				})
				.unwrap_or(false)
		})
	}

	fn materialize_task_spec(
		&self,
		mut spec: TaskSpec,
		stages: &[StagePlan],
	) -> Result<TaskSpec, SchedulerError> {
		let runtime_filter = self.runtime_filter_descriptor_for_probe(&spec, stages);
		let consumer_streamable = is_streamable_shuffle_consumer(&spec);
		let consumer_streamable_both = !contains_broadcast_join(&spec.plan)
			&& spec_all_shuffle_inputs_from_streamable_producer_stages(&spec, stages);
		for input in &mut spec.inputs {
			if let TaskInputBinding::Scan {
				runtime_filter: input_runtime_filter,
				..
			} = input
			{
				if input_runtime_filter.is_none() {
					*input_runtime_filter = runtime_filter.clone();
				}
				continue;
			}

			let TaskInputBinding::ShuffleConsumer {
				exchange_id,
				producer_stage_id,
				partition_id,
				producer_partitions,
			} = input
			else {
				continue;
			};

			if !producer_partitions.is_empty() {
				continue;
			}

			let producers =
				expected_producer_tasks(stages, *producer_stage_id).ok_or_else(|| {
					SchedulerError::PartitionManifestMismatch {
						message: format!("missing producer stage {:?}", producer_stage_id),
					}
				})?;
			let mut refs = Vec::with_capacity(producers.len());
			for producer in producers {
				if let Some(sealed) =
					self.sealed_ref_for_producer(&producer.spec, *exchange_id, *partition_id)
				{
					refs.push(ShufflePartitionRef::Sealed(sealed));
					continue;
				}
				if producers.len() == 1
					&& (consumer_streamable || consumer_streamable_both)
					&& let Some(live) =
						self.live_ref_for_producer(&producer.spec, *exchange_id, *partition_id)
				{
					refs.push(ShufflePartitionRef::Live(live));
					continue;
				}
				return Err(SchedulerError::PartitionManifestMismatch {
					message: format!(
						"shuffle dependency for ExchangeId {:?} stage {:?} partition {:?} is not ready",
						exchange_id, producer_stage_id, partition_id
					),
				});
			}

			*producer_partitions = refs;
		}

		Ok(spec)
	}

	fn sealed_ref_for_producer(
		&self,
		producer: &TaskSpec,
		exchange_id: crate::distributed::identity::ExchangeId,
		partition_id: crate::distributed::identity::PartitionId,
	) -> Option<ProducerPartitionRef> {
		let key = TaskKey::from_identity(producer.identity);
		let manifest = self.accepted_manifests.get(&key)?;
		if manifest.exchange_id != exchange_id {
			return None;
		}
		manifest
			.partitions
			.iter()
			.find(|entry| entry.partition_id == partition_id)
			.map(|entry| ProducerPartitionRef {
				address: PartitionAddress {
					query_id: manifest.query_id,
					exchange_id: manifest.exchange_id,
					producer_stage_id: manifest.stage_id,
					producer_task_id: manifest.task_id,
					attempt_id: manifest.attempt_id,
					partition_id: entry.partition_id,
				},
				worker_id: manifest.producer_worker_id,
				endpoint: manifest.producer_endpoint.clone(),
				size_bytes: entry.size_bytes,
				digest: entry.digest,
			})
	}

	fn live_ref_for_producer(
		&self,
		producer: &TaskSpec,
		exchange_id: crate::distributed::identity::ExchangeId,
		partition_id: crate::distributed::identity::PartitionId,
	) -> Option<LiveProducerPartitionRef> {
		if !is_streamable_exchange_producer(producer) {
			return None;
		}
		let key = TaskKey::from_identity(producer.identity);
		let (identity, worker_id) = self.state.running_attempt(key)?;
		let worker = self.registry.resolve(worker_id)?;
		Some(LiveProducerPartitionRef {
			address: PartitionAddress {
				query_id: identity.query_id,
				exchange_id,
				producer_stage_id: identity.stage_id,
				producer_task_id: identity.task_id,
				attempt_id: identity.attempt_id,
				partition_id,
			},
			worker_id,
			endpoint: worker.endpoint.clone(),
		})
	}

	fn maybe_attach_runtime_filter_fragment(
		&mut self,
		spec: &TaskSpec,
		stages: &[StagePlan],
		result_batches: &[RecordBatch],
		manifest: &mut PartitionManifest,
	) -> Result<(), SchedulerError> {
		if !runtime_filter_build_stage(spec, stages) {
			return Ok(());
		}
		if let Some(fragment) = manifest.runtime_filter_fragment.clone() {
			self.record_runtime_filter_fragment(
				spec.identity.query_id,
				spec.identity.stage_id,
				fragment,
			);
			return Ok(());
		}
		if result_batches.is_empty() {
			return Ok(());
		}
		let TaskOutputBinding::ShuffleProducer { partition_key, .. } = &spec.output else {
			return Ok(());
		};
		let Some(fragment) =
			build_runtime_filter_fragment(result_batches, partition_key).map_err(|err| {
				SchedulerError::Acceptance(format!("runtime filter generation failed: {err:?}"))
			})?
		else {
			return Ok(());
		};
		manifest.runtime_filter_fragment = Some(fragment.clone());
		self.record_runtime_filter_fragment(
			spec.identity.query_id,
			spec.identity.stage_id,
			fragment,
		);
		Ok(())
	}

	fn record_runtime_filter_fragment(
		&mut self,
		query_id: QueryId,
		stage_id: StageId,
		fragment: crate::distributed::task::RuntimeFilterFragment,
	) {
		match self.runtime_filter_merges.entry((query_id, stage_id)) {
			std::collections::hash_map::Entry::Vacant(entry) => {
				entry.insert(RuntimeFilterMerge::Active(fragment));
			}
			std::collections::hash_map::Entry::Occupied(mut entry) => {
				if let RuntimeFilterMerge::Active(active) = entry.get_mut()
					&& !merge_fragments(active, &fragment)
				{
					entry.insert(RuntimeFilterMerge::Dropped);
				}
			}
		}
	}

	fn runtime_filter_descriptor_for_probe(
		&self,
		spec: &TaskSpec,
		stages: &[StagePlan],
	) -> Option<wendelon::schema::RuntimeFilterDescriptor> {
		let stage = stages
			.iter()
			.find(|stage| stage.stage_id == spec.identity.stage_id)?;
		if !matches!(spec.output, TaskOutputBinding::ShuffleProducer { .. })
			|| stage.upstream_stages.len() != 1
		{
			return None;
		}
		let build_stage_id = stage.upstream_stages[0];
		let RuntimeFilterMerge::Active(fragment) = self
			.runtime_filter_merges
			.get(&(spec.identity.query_id, build_stage_id))?
		else {
			return None;
		};
		let TaskOutputBinding::ShuffleProducer { partition_key, .. } = &spec.output else {
			return None;
		};
		let column = partition_key.single_column_root()?;
		let probe_descriptor_fingerprint = first_scan_fingerprint(&spec.plan)?;
		Some(descriptor_from_fragment(
			fragment,
			vec![column.ordinal.0],
			probe_descriptor_fingerprint,
		))
	}

	fn refresh_drained_producers(&mut self, stages: &[StagePlan]) {
		let drained_keys = self
			.accepted_manifests
			.keys()
			.copied()
			.filter(|key| self.producer_consumers_transitively_accepted(*key, stages))
			.collect::<Vec<_>>();

		for key in drained_keys {
			let _ = self.state.mark_drained(key);
		}
	}

	fn producer_consumers_transitively_accepted(&self, key: TaskKey, stages: &[StagePlan]) -> bool {
		let downstream_stages = stages
			.iter()
			.filter(|stage| stage.upstream_stages.contains(&key.stage_id))
			.collect::<Vec<_>>();
		if downstream_stages.is_empty() {
			return false;
		}

		downstream_stages
			.iter()
			.all(|stage| self.stage_transitively_accepted(stage.stage_id, stages))
	}

	fn stage_transitively_accepted(&self, stage_id: StageId, stages: &[StagePlan]) -> bool {
		let Some(stage) = stages.iter().find(|stage| stage.stage_id == stage_id) else {
			return false;
		};
		let stage_accepted = stage.tasks.iter().all(|task| {
			matches!(
				self.state
					.acceptance_state(TaskKey::from_identity(task.spec.identity)),
				AcceptanceState::LiveAccepted { .. }
			)
		});
		if !stage_accepted {
			return false;
		}

		stages
			.iter()
			.filter(|child| child.upstream_stages.contains(&stage_id))
			.all(|child| self.stage_transitively_accepted(child.stage_id, stages))
	}
}

fn task_dependencies(spec: &TaskSpec) -> Vec<PartitionAddress> {
	spec.inputs
		.iter()
		.flat_map(|input| match input {
			TaskInputBinding::Scan { .. } => Vec::new(),
			TaskInputBinding::ShuffleConsumer {
				producer_partitions,
				..
			} => producer_partitions
				.iter()
				.map(ShufflePartitionRef::address)
				.collect(),
		})
		.collect()
}

fn expected_producer_tasks(
	stages: &[StagePlan],
	producer_stage_id: StageId,
) -> Option<&[TaskPlan]> {
	stages
		.iter()
		.find(|stage| stage.stage_id == producer_stage_id)
		.map(|stage| stage.tasks.as_slice())
}

pub(crate) fn producer_stage_supports_live(stage_id: StageId, stages: &[StagePlan]) -> bool {
	stages
		.iter()
		.find(|stage| stage.stage_id == stage_id)
		.is_some_and(|stage| {
			stage.tasks.len() == 1 && exchange_producer_plan_supports_live(&stage.tasks[0].spec)
		})
}

fn is_live_publishing_producer_stage(stage_id: StageId, stages: &[StagePlan]) -> bool {
	stages
		.iter()
		.find(|stage| stage.stage_id == stage_id)
		.is_some_and(|stage| {
			stage.tasks.len() == 1 && is_streamable_exchange_producer(&stage.tasks[0].spec)
		})
}

pub(crate) fn consumer_supports_live_inputs(
	stage: &StagePlan,
	task: &TaskPlan,
	stages: &[StagePlan],
) -> bool {
	consumer_supports_live_inputs_when(stage, task, |stage_id| {
		is_live_publishing_producer_stage(stage_id, stages)
	})
}

pub(crate) fn consumer_supports_live_candidate_inputs(
	stage: &StagePlan,
	task: &TaskPlan,
	live_stage_ids: &BTreeSet<StageId>,
) -> bool {
	consumer_supports_live_inputs_when(stage, task, |stage_id| live_stage_ids.contains(&stage_id))
}

/// ADR 536 Phase 0 observation: what a resolved stage graph's live publication would ask
/// of admission. Observe-only — it reads the graph and changes nothing.
///
/// `live_eligible_consumer_tasks` is the authoritative count: it consumes the same
/// predicate the scheduler dispatches on, so an ADR 123 multi-upstream join consumer is
/// counted as live-eligible rather than overlooked by the narrower unary shape test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivePublicationReport {
	pub stage_upstreams: Vec<(StageId, Vec<StageId>)>,
	pub live_producer_stages: Vec<StageId>,
	pub live_eligible_consumer_tasks: usize,
	pub multi_upstream_live_eligible_consumer_tasks: usize,
	/// Largest number of tasks that must be resident together for some live component to
	/// drain. A live producer holds its slot until it seals, so every task of every stage
	/// reachable across live edges has to fit under the cap at once.
	pub live_residency_requirement: usize,
	pub max_in_flight_tasks: Option<usize>,
	pub residency_exceeds_admission: bool,
}

/// Describe the live publication structure of an already-resolved stage graph against an
/// admission cap. Phase 0 records this; it does not act on it.
pub fn live_publication_report(
	stages: &[StagePlan],
	max_in_flight_tasks: Option<usize>,
) -> LivePublicationReport {
	let stage_upstreams = stages
		.iter()
		.map(|stage| (stage.stage_id, stage.upstream_stages.clone()))
		.collect::<Vec<_>>();

	let live_producer_stages = stages
		.iter()
		.filter(|stage| stage_publishes_live(stage))
		.map(|stage| stage.stage_id)
		.collect::<Vec<_>>();

	let mut live_eligible_consumer_tasks = 0;
	let mut multi_upstream_live_eligible_consumer_tasks = 0;
	for stage in stages {
		for task in &stage.tasks {
			let consumes_shuffle = task
				.spec
				.inputs
				.iter()
				.any(|input| matches!(input, TaskInputBinding::ShuffleConsumer { .. }));
			// Eligibility means consuming a producer that is actually publishing live, not
			// merely having a shape that could support one. A unary consumer reading a
			// sealed producer drains nothing concurrently and must not arm the stall
			// premise. The multi-upstream arm already requires this of every upstream; the
			// unary arm tests only the consumer's own shape, so it is checked here.
			if !consumes_shuffle
				|| !consumer_supports_live_inputs(stage, task, stages)
				|| !task_consumes_only_live_publishing_producers(task, stages)
			{
				continue;
			}
			live_eligible_consumer_tasks += 1;
			if stage.upstream_stages.len() > 1 {
				multi_upstream_live_eligible_consumer_tasks += 1;
			}
		}
	}

	let live_residency_requirement = live_components(stages)
		.into_iter()
		.map(|component| {
			component
				.iter()
				.filter_map(|stage_id| stages.iter().find(|stage| stage.stage_id == *stage_id))
				.map(|stage| stage.tasks.len())
				.sum::<usize>()
		})
		.max()
		.unwrap_or(0);

	let residency_exceeds_admission =
		max_in_flight_tasks.is_some_and(|limit| live_residency_requirement > limit);

	LivePublicationReport {
		stage_upstreams,
		live_producer_stages,
		live_eligible_consumer_tasks,
		multi_upstream_live_eligible_consumer_tasks,
		live_residency_requirement,
		max_in_flight_tasks,
		residency_exceeds_admission,
	}
}

/// Every shuffle input of this task is served by a stage that is actually publishing live.
fn task_consumes_only_live_publishing_producers(task: &TaskPlan, stages: &[StagePlan]) -> bool {
	task.spec.inputs.iter().all(|input| match input {
		TaskInputBinding::ShuffleConsumer {
			producer_stage_id, ..
		} => stages
			.iter()
			.find(|stage| stage.stage_id == *producer_stage_id)
			.is_some_and(stage_publishes_live),
		TaskInputBinding::Scan { .. } => true,
	})
}

fn stage_publishes_live(stage: &StagePlan) -> bool {
	stage.tasks.iter().any(|task| {
		matches!(
			task.spec.output,
			TaskOutputBinding::ShuffleProducer {
				publish_mode: ShufflePublishMode::Live,
				..
			}
		)
	})
}

/// Stage sets connected by live shuffle edges. A live producer and every stage that would
/// drain it concurrently must be resident together, so they share one residency budget.
fn live_components(stages: &[StagePlan]) -> Vec<BTreeSet<StageId>> {
	let mut components: Vec<BTreeSet<StageId>> = Vec::new();

	for producer in stages.iter().filter(|stage| stage_publishes_live(stage)) {
		let mut component = BTreeSet::from([producer.stage_id]);
		let mut frontier = vec![producer.stage_id];
		while let Some(stage_id) = frontier.pop() {
			for consumer in stages
				.iter()
				.filter(|stage| stage.upstream_stages.contains(&stage_id))
			{
				if component.contains(&consumer.stage_id) {
					continue;
				}
				component.insert(consumer.stage_id);
				// A live consumer that is itself a live producer extends the same
				// component: its own consumers must also be resident.
				if stage_publishes_live(consumer) {
					frontier.push(consumer.stage_id);
				}
			}
		}
		components.push(component);
	}

	// Components sharing a stage are one residency budget, not two.
	let mut merged: Vec<BTreeSet<StageId>> = Vec::new();
	for component in components {
		match merged
			.iter_mut()
			.find(|existing| !existing.is_disjoint(&component))
		{
			Some(existing) => existing.extend(component),
			None => merged.push(component),
		}
	}
	merged
}

fn consumer_supports_live_inputs_when(
	stage: &StagePlan,
	task: &TaskPlan,
	producer_is_live: impl Fn(StageId) -> bool,
) -> bool {
	if contains_broadcast_join(&task.spec.plan) {
		return false;
	}
	match stage.upstream_stages.len() {
		1 => is_streamable_shuffle_consumer(&task.spec),
		0 => false,
		_ => stage
			.upstream_stages
			.iter()
			.all(|stage_id| producer_is_live(*stage_id)),
	}
}

fn spec_all_shuffle_inputs_from_streamable_producer_stages(
	spec: &TaskSpec,
	stages: &[StagePlan],
) -> bool {
	let producer_stage_ids = spec
		.inputs
		.iter()
		.filter_map(|input| match input {
			TaskInputBinding::ShuffleConsumer {
				producer_stage_id, ..
			} => Some(*producer_stage_id),
			TaskInputBinding::Scan { .. } => None,
		})
		.collect::<BTreeSet<_>>();
	producer_stage_ids.len() > 1
		&& producer_stage_ids
			.iter()
			.all(|stage_id| is_live_publishing_producer_stage(*stage_id, stages))
}

fn contains_broadcast_join(plan: &ExecutionPlan) -> bool {
	match plan {
		ExecutionPlan::Join {
			left,
			right,
			strategy,
			..
		} => {
			*strategy == JoinStrategy::Broadcast
				|| contains_broadcast_join(left)
				|| contains_broadcast_join(right)
		}
		ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Repeat { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. } => contains_broadcast_join(input),
		ExecutionPlan::Union { left, right, .. } => {
			contains_broadcast_join(left) || contains_broadcast_join(right)
		}
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::Scan { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE is one self-contained local task — opaque to scheduler walkers.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => false,
	}
}

fn runtime_filter_build_stage(spec: &TaskSpec, stages: &[StagePlan]) -> bool {
	matches!(spec.output, TaskOutputBinding::ShuffleProducer { .. })
		&& stages.iter().any(|stage| {
			stage.upstream_stages.as_slice() == [spec.identity.stage_id]
				&& stage.tasks.iter().any(|task| {
					matches!(task.spec.output, TaskOutputBinding::ShuffleProducer { .. })
				})
		})
}

fn first_scan_fingerprint(plan: &ExecutionPlan) -> Option<PhysicalDescriptorFingerprint> {
	match plan {
		ExecutionPlan::Scan { fingerprint, .. } => Some(*fingerprint),
		ExecutionPlan::Filter { input, .. }
		| ExecutionPlan::Project { input, .. }
		| ExecutionPlan::Aggregate { input, .. }
		| ExecutionPlan::Exchange { input, .. }
		| ExecutionPlan::Repeat { input, .. }
		| ExecutionPlan::Sort { input, .. }
		| ExecutionPlan::Window { input, .. }
		| ExecutionPlan::Take { input, .. } => first_scan_fingerprint(input),
		ExecutionPlan::Join { left, right, .. } => {
			first_scan_fingerprint(left).or_else(|| first_scan_fingerprint(right))
		}
		ExecutionPlan::Union { left, right, .. } => {
			first_scan_fingerprint(left).or_else(|| first_scan_fingerprint(right))
		}
		ExecutionPlan::EmptyRelation { .. }
		| ExecutionPlan::SystemTable { .. }
		| ExecutionPlan::CreateTable { .. }
		| ExecutionPlan::CreateLogicalType { .. }
		| ExecutionPlan::CreateTableAsSelect { .. }
		| ExecutionPlan::DropTable { .. }
		| ExecutionPlan::Delete { .. }
		| ExecutionPlan::Update { .. }
		| ExecutionPlan::Insert { .. }
		| ExecutionPlan::InsertSelect { .. }
		// ADR 255: a recursive CTE is one self-contained local task — opaque to scheduler walkers.
		| ExecutionPlan::RecursiveCte { .. }
		| ExecutionPlan::RecursiveRef { .. } => None,
	}
}

fn failure_retryable(kind: &TaskFailureKind) -> bool {
	match kind {
		TaskFailureKind::TaskFailed { retryable, .. } => *retryable,
		TaskFailureKind::BudgetExhausted { dimension, .. } => dimension.retryable(),
		_ => false,
	}
}

fn duration_micros(started: Instant) -> u64 {
	u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

fn task_failure_is_stale_rejection(kind: &TaskFailureKind) -> bool {
	match kind {
		TaskFailureKind::Internal { reason } | TaskFailureKind::TaskFailed { reason, .. } => {
			reason.contains("AttemptStale")
		}
		_ => false,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::distributed::budget::{ByteCount, QueryBudget, TaskBudget};
	use crate::distributed::identity::{ExchangeId, PartitionId, QueryId, TaskId, WorkerId};
	use crate::distributed::task::{
		ArrowIpcDigest, PartitionBackend, PartitionEntry, RowCount, RuntimeFilterFragment,
		TaskInputBinding, TaskOutputBinding,
	};
	use crate::distributed::worker_registry::{
		ActiveTaskCount, SimdLaneCount, StaticWorkerRegistry, UnixMillis, WorkerCapabilities,
		WorkerEndpoint, WorkerLease, WorkerResourceState, WorkerStatus,
	};
	use crate::ir::{
		BoundExprProgram, BoundExprProgramBuilder, ColumnName, ColumnOrdinal, ExecutionPlan,
		JoinKind, JoinStrategy, ResolvedColumn,
	};
	use arrow::array::{ArrayRef, Int64Array};
	use arrow::datatypes::{DataType, Field, Schema};
	use std::collections::{HashSet, VecDeque};
	use std::sync::Arc;
	use std::sync::Mutex;
	use std::sync::atomic::{AtomicBool, Ordering};
	use tokio::sync::Notify;
	use wendelon::schema::{
		LocalityPolicy, PhysicalDescriptorFingerprint, PrimaryKeyDescriptor, RowIdentityPolicy,
		SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor,
	};

	fn worker_entry(worker_id: WorkerId, active_tasks: ActiveTaskCount) -> WorkerEntry {
		WorkerEntry {
			worker_id,
			endpoint: WorkerEndpoint::new("127.0.0.1:0"),
			lease: WorkerLease {
				last_heartbeat: UnixMillis::new(1),
				epoch: crate::distributed::identity::LeaseEpoch::new(1),
			},
			capabilities: WorkerCapabilities {
				advertised_memory: ByteCount::new(1024),
				simd_width: SimdLaneCount::new(8),
				spill_capacity_reserved: ByteCount::new(1024),
			},
			resources: WorkerResourceState {
				in_flight: ByteCount::ZERO,
				active_tasks,
			},
			status: WorkerStatus::Live,
		}
	}

	fn registry() -> StaticWorkerRegistry {
		StaticWorkerRegistry::new([worker_entry(
			WorkerId::from_u128(1),
			ActiveTaskCount::default(),
		)])
	}

	fn failover_registry() -> StaticWorkerRegistry {
		StaticWorkerRegistry::new([
			worker_entry(WorkerId::from_u128(1), ActiveTaskCount::default()),
			worker_entry(WorkerId::from_u128(2), ActiveTaskCount::new(1)),
		])
	}

	fn task(stage_id: StageId, task_id: TaskId) -> TaskPlan {
		TaskPlan::new(TaskSpec {
			identity: TaskIdentity::new(
				QueryId::from_u128(10),
				stage_id,
				task_id,
				AttemptId::first(),
			),
			partition: None,
			context: crate::distributed::context::TaskExecutionContext::local_test(),
			join_runtime_observation_mode:
				crate::distributed::task::JoinRuntimeObservationMode::Disabled,
			plan: ExecutionPlan::Scan {
				local_table_id: crate::ir::TableId(1),
				table: TableObjectId::new([1; 16]),
				fingerprint: physical_fingerprint(),
				schema: Arc::new(Schema::empty()),
				projection: None,
				pbc_program: None,
				row_identity: crate::storage::RowIdentityProjection::None,
				row_limit: None,
				profile_hint: crate::ir::QueryProfileHint::Scan,
				estimated_byte_size: None,
				primary_key_columns: Vec::new(),
				column_ndv: Vec::new(),
				column_range: Vec::new(),
				column_range_distributions: Vec::new(),
				column_heavy_hitters: Vec::new(),
				column_group_top_k: Default::default(),
				column_group_ndv: Default::default(),
			},
			inputs: vec![TaskInputBinding::Scan {
				share: None,
				runtime_filter: None,
			}],
			output: TaskOutputBinding::FinalResult,
			budget: TaskBudget::from_query_budget(QueryBudget::test_default()),
			trace: None,
		})
	}

	fn physical_fingerprint() -> PhysicalDescriptorFingerprint {
		let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
		TableSchemaDescriptor::new(
			TableObjectId::new([3; 16]),
			1,
			"default",
			"scheduler_test",
			&schema,
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicy::AdditiveOnly,
		)
		.expect("descriptor")
		.physical_descriptor_fingerprint
	}

	struct ScriptedDispatcher {
		outcomes: Mutex<VecDeque<DispatchOutcome>>,
		seen: Mutex<Vec<TaskSpec>>,
	}

	impl ScriptedDispatcher {
		fn new(outcomes: impl IntoIterator<Item = DispatchOutcome>) -> Self {
			Self {
				outcomes: Mutex::new(outcomes.into_iter().collect()),
				seen: Mutex::new(Vec::new()),
			}
		}

		fn seen(&self) -> Vec<TaskSpec> {
			self.seen.lock().expect("seen lock").clone()
		}
	}

	#[async_trait]
	impl TaskDispatcher for ScriptedDispatcher {
		async fn dispatch(&self, _worker: &WorkerEntry, spec: &TaskSpec) -> DispatchOutcome {
			self.seen.lock().expect("seen lock").push(spec.clone());
			self.outcomes
				.lock()
				.expect("outcomes lock")
				.pop_front()
				.expect("scripted outcome")
		}
	}

	fn completed() -> DispatchOutcome {
		DispatchOutcome::Completed(TaskCompletionOutput::FinalResult {
			output_digest: None,
		})
	}

	struct BlockingFirstDispatcher {
		seen: Mutex<Vec<TaskIdentity>>,
		seen_notify: Notify,
		release_first: Notify,
		first_released: AtomicBool,
	}

	impl BlockingFirstDispatcher {
		fn new() -> Self {
			Self {
				seen: Mutex::new(Vec::new()),
				seen_notify: Notify::new(),
				release_first: Notify::new(),
				first_released: AtomicBool::new(false),
			}
		}

		async fn wait_for_seen(&self, count: usize) {
			loop {
				if self.seen.lock().expect("seen lock").len() >= count {
					return;
				}
				self.seen_notify.notified().await;
			}
		}

		fn release_first(&self) {
			self.first_released.store(true, Ordering::SeqCst);
			self.release_first.notify_waiters();
		}

		fn seen(&self) -> Vec<TaskIdentity> {
			self.seen.lock().expect("seen lock").clone()
		}
	}

	#[async_trait]
	impl TaskDispatcher for BlockingFirstDispatcher {
		async fn dispatch(&self, _worker: &WorkerEntry, spec: &TaskSpec) -> DispatchOutcome {
			self.seen.lock().expect("seen lock").push(spec.identity);
			self.seen_notify.notify_waiters();
			if spec.identity.task_id == TaskId::new(0) {
				loop {
					let notified = self.release_first.notified();
					tokio::pin!(notified);
					notified.as_mut().enable();
					if self.first_released.load(Ordering::SeqCst) {
						break;
					}
					notified.await;
				}
			}
			completed()
		}
	}

	struct BlockingProducerDispatcher {
		seen: Mutex<Vec<TaskSpec>>,
		seen_notify: Notify,
		release: Notify,
		blocked_task: TaskKey,
		exchange_id: ExchangeId,
	}

	struct BlockingTaskSetDispatcher {
		seen: Mutex<Vec<TaskSpec>>,
		seen_notify: Notify,
		release: Notify,
		blocked_tasks: HashSet<TaskKey>,
		released: AtomicBool,
	}

	impl BlockingTaskSetDispatcher {
		fn new(blocked_tasks: impl IntoIterator<Item = TaskKey>) -> Self {
			Self {
				seen: Mutex::new(Vec::new()),
				seen_notify: Notify::new(),
				release: Notify::new(),
				blocked_tasks: blocked_tasks.into_iter().collect(),
				released: AtomicBool::new(false),
			}
		}

		async fn wait_for_seen(&self, count: usize) {
			loop {
				if self.seen.lock().expect("seen lock").len() >= count {
					return;
				}
				self.seen_notify.notified().await;
			}
		}

		fn release(&self) {
			self.released.store(true, Ordering::SeqCst);
			self.release.notify_waiters();
		}

		fn seen(&self) -> Vec<TaskSpec> {
			self.seen.lock().expect("seen lock").clone()
		}
	}

	#[async_trait]
	impl TaskDispatcher for BlockingTaskSetDispatcher {
		async fn dispatch(&self, _worker: &WorkerEntry, spec: &TaskSpec) -> DispatchOutcome {
			self.seen.lock().expect("seen lock").push(spec.clone());
			self.seen_notify.notify_waiters();
			if self
				.blocked_tasks
				.contains(&TaskKey::from_identity(spec.identity))
			{
				loop {
					let notified = self.release.notified();
					tokio::pin!(notified);
					notified.as_mut().enable();
					if self.released.load(Ordering::SeqCst) {
						break;
					}
					notified.await;
				}
			}
			match spec.output {
				TaskOutputBinding::ShuffleProducer { exchange_id, .. } => {
					DispatchOutcome::Completed(TaskCompletionOutput::ShuffleProducer {
						manifest: manifest_for_spec(spec, exchange_id),
					})
				}
				TaskOutputBinding::FinalResult => completed(),
			}
		}
	}

	impl BlockingProducerDispatcher {
		fn new(blocked_task: TaskKey, exchange_id: ExchangeId) -> Self {
			Self {
				seen: Mutex::new(Vec::new()),
				seen_notify: Notify::new(),
				release: Notify::new(),
				blocked_task,
				exchange_id,
			}
		}

		async fn wait_for_seen(&self, count: usize) {
			loop {
				if self.seen.lock().expect("seen lock").len() >= count {
					return;
				}
				self.seen_notify.notified().await;
			}
		}

		fn release(&self) {
			self.release.notify_waiters();
		}

		fn seen(&self) -> Vec<TaskSpec> {
			self.seen.lock().expect("seen lock").clone()
		}
	}

	#[async_trait]
	impl TaskDispatcher for BlockingProducerDispatcher {
		async fn dispatch(&self, _worker: &WorkerEntry, spec: &TaskSpec) -> DispatchOutcome {
			self.seen.lock().expect("seen lock").push(spec.clone());
			self.seen_notify.notify_waiters();
			if TaskKey::from_identity(spec.identity) == self.blocked_task {
				self.release.notified().await;
			}
			match spec.output {
				TaskOutputBinding::ShuffleProducer { .. } => {
					DispatchOutcome::Completed(TaskCompletionOutput::ShuffleProducer {
						manifest: manifest_for_spec(spec, self.exchange_id),
					})
				}
				TaskOutputBinding::FinalResult => completed(),
			}
		}
	}

	fn int64_partition_key() -> BoundExprProgram {
		let mut builder = BoundExprProgramBuilder::new();
		let id = builder.append_column(ResolvedColumn {
			name: ColumnName("id".to_string()),
			ordinal: ColumnOrdinal(0),
			data_type: DataType::Int64,
			nullable: false,
			pbc_eligible: true,
		});
		builder.push_root(id);
		builder.build()
	}

	fn runtime_filter_fragment(payload: Vec<u8>, hash_count: u8) -> RuntimeFilterFragment {
		RuntimeFilterFragment {
			key_encoding_version: 1,
			key_types: vec![wendelon::schema::RuntimeFilterKeyType::Int64],
			kind: wendelon::schema::RuntimeFilterKind::Bloom(
				wendelon::schema::RuntimeFilterBloom {
					hash_seed: 7,
					bit_count: 16,
					hash_count,
					byte_budget: u32::try_from(payload.len()).expect("payload len fits u32"),
					target_fp_rate_bp: 100,
					payload,
				},
			),
		}
	}

	fn manifest_for_spec(spec: &TaskSpec, exchange_id: ExchangeId) -> PartitionManifest {
		PartitionManifest {
			query_id: spec.identity.query_id,
			stage_id: spec.identity.stage_id,
			task_id: spec.identity.task_id,
			attempt_id: spec.identity.attempt_id,
			exchange_id,
			producer_worker_id: WorkerId::from_u128(1),
			producer_endpoint: WorkerEndpoint::new("127.0.0.1:9191"),
			partitions: vec![PartitionEntry {
				partition_id: PartitionId::new(0),
				size_bytes: ByteCount::new(8),
				row_count: RowCount::new(1),
				digest: ArrowIpcDigest::new([4; 32]),
			}],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		}
	}

	fn streamable_shuffle_producer(
		stage_id: StageId,
		task_id: TaskId,
		exchange_id: ExchangeId,
	) -> TaskPlan {
		let mut producer = task(stage_id, task_id);
		producer.spec.plan = ExecutionPlan::Exchange {
			input: Box::new(producer.spec.plan.clone()),
			partition_count: 1,
			key: BoundExprProgram::empty(),
		};
		producer.spec.output = TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count: crate::distributed::task::PartitionCount::new(1),
			partition_key: BoundExprProgram::empty(),
			publish_mode: crate::distributed::task::ShufflePublishMode::Live,
		};
		producer
	}

	fn non_streamable_shuffle_producer(
		stage_id: StageId,
		task_id: TaskId,
		exchange_id: ExchangeId,
	) -> TaskPlan {
		let mut producer = streamable_shuffle_producer(stage_id, task_id, exchange_id);
		let schema = producer.spec.plan.schema();
		producer.spec.plan = ExecutionPlan::Exchange {
			input: Box::new(ExecutionPlan::Aggregate {
				input: Box::new(task(stage_id, task_id).spec.plan),
				group_expr: BoundExprProgram::empty(),
				aggr_expr: Vec::new(),
				output_schema: schema,
			}),
			partition_count: 1,
			key: BoundExprProgram::empty(),
		};
		producer
	}

	fn shuffle_consumer(
		stage_id: StageId,
		exchange_id: ExchangeId,
		producer_stage_id: StageId,
	) -> TaskPlan {
		let mut consumer = task(stage_id, TaskId::new(0));
		consumer.spec.plan = ExecutionPlan::Scan {
			local_table_id: crate::distributed::task::shuffle_input_table_id(exchange_id),
			table: TableObjectId::new([9; 16]),
			fingerprint: physical_fingerprint(),
			schema: Arc::new(Schema::empty()),
			projection: None,
			pbc_program: None,
			row_identity: crate::storage::RowIdentityProjection::None,
			row_limit: None,
			profile_hint: crate::ir::QueryProfileHint::Scan,
			estimated_byte_size: None,
			primary_key_columns: Vec::new(),
			column_ndv: Vec::new(),
			column_range: Vec::new(),
			column_range_distributions: Vec::new(),
			column_heavy_hitters: Vec::new(),
			column_group_top_k: Default::default(),
			column_group_ndv: Default::default(),
		};
		consumer.spec.inputs = vec![TaskInputBinding::ShuffleConsumer {
			exchange_id,
			producer_stage_id,
			partition_id: PartitionId::new(0),
			producer_partitions: Vec::new(),
		}];
		consumer
	}

	fn shuffle_input_scan(exchange_id: ExchangeId, object_byte: u8) -> ExecutionPlan {
		let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
		ExecutionPlan::Scan {
			local_table_id: crate::distributed::task::shuffle_input_table_id(exchange_id),
			table: TableObjectId::new([object_byte; 16]),
			fingerprint: physical_fingerprint(),
			schema,
			projection: None,
			pbc_program: None,
			row_identity: crate::storage::RowIdentityProjection::None,
			row_limit: None,
			profile_hint: crate::ir::QueryProfileHint::Scan,
			estimated_byte_size: None,
			primary_key_columns: vec![ColumnOrdinal(0)],
			column_ndv: Vec::new(),
			column_range: Vec::new(),
			column_range_distributions: Vec::new(),
			column_heavy_hitters: Vec::new(),
			column_group_top_k: Default::default(),
			column_group_ndv: Default::default(),
		}
	}

	fn hash_join_consumer(
		stage_id: StageId,
		left_exchange_id: ExchangeId,
		left_producer_stage_id: StageId,
		right_exchange_id: ExchangeId,
		right_producer_stage_id: StageId,
		kind: JoinKind,
	) -> TaskPlan {
		let mut consumer = task(stage_id, TaskId::new(0));
		let left = shuffle_input_scan(left_exchange_id, 7);
		let right = shuffle_input_scan(right_exchange_id, 8);
		let left_schema = left.schema();
		let right_schema = right.schema();
		consumer.spec.plan = ExecutionPlan::Join {
			replay_id: JoinReplayId::SYNTHETIC,
			replay_eligibility: BroadcastReplayEligibility::Terminal,
			left: Box::new(left),
			right: Box::new(right),
			kind,
			left_keys: vec![ColumnOrdinal(0)],
			right_keys: vec![ColumnOrdinal(0)],
			match_filter: None,
			null_safe_keys: false,
			strategy: JoinStrategy::HashPartitioned,
			output_schema: crate::binder::join_schema(kind, &left_schema, &right_schema),
		};
		consumer.spec.inputs = vec![
			TaskInputBinding::ShuffleConsumer {
				exchange_id: left_exchange_id,
				producer_stage_id: left_producer_stage_id,
				partition_id: PartitionId::new(0),
				producer_partitions: Vec::new(),
			},
			TaskInputBinding::ShuffleConsumer {
				exchange_id: right_exchange_id,
				producer_stage_id: right_producer_stage_id,
				partition_id: PartitionId::new(0),
				producer_partitions: Vec::new(),
			},
		];
		consumer
	}

	async fn assert_multi_upstream_streamable_join_dispatches_before_acceptance(
		assert_manifest_absent: bool,
	) {
		let left_exchange_id = ExchangeId::new(1231);
		let right_exchange_id = ExchangeId::new(1232);
		let left = streamable_shuffle_producer(StageId::new(0), TaskId::new(0), left_exchange_id);
		let right = streamable_shuffle_producer(StageId::new(1), TaskId::new(0), right_exchange_id);
		let left_key = TaskKey::from_identity(left.spec.identity);
		let right_key = TaskKey::from_identity(right.spec.identity);
		let consumer = hash_join_consumer(
			StageId::new(2),
			left_exchange_id,
			StageId::new(0),
			right_exchange_id,
			StageId::new(1),
			JoinKind::Inner,
		);
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stages: vec![],
				tasks: vec![left],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stages: vec![],
				tasks: vec![right],
			},
			StagePlan {
				stage_id: StageId::new(2),
				upstream_stages: vec![StageId::new(0), StageId::new(1)],
				tasks: vec![consumer],
			},
		];
		let mut scheduler = DistributedScheduler::new(registry());
		let dispatcher = BlockingTaskSetDispatcher::new([left_key, right_key]);
		let dispatch = scheduler.dispatch_loop(&stages, &dispatcher);
		tokio::pin!(dispatch);

		tokio::select! {
			seen = tokio::time::timeout(
				std::time::Duration::from_secs(1),
				dispatcher.wait_for_seen(3),
			) => {
				seen.expect("consumer should dispatch while both upstreams are still running");
			}
			result = &mut dispatch => {
				panic!("dispatch completed before live multi-upstream consumer was observed: {result:?}");
			}
		}
		if assert_manifest_absent {
			assert!(
				!dispatcher.released.load(Ordering::SeqCst),
				"blocked producers cannot have returned manifests before release"
			);
		}
		let seen = dispatcher.seen();
		assert_eq!(seen[0].identity.stage_id, StageId::new(0));
		assert_eq!(seen[1].identity.stage_id, StageId::new(1));
		assert_eq!(seen[2].identity.stage_id, StageId::new(2));
		for input in &seen[2].inputs {
			let TaskInputBinding::ShuffleConsumer {
				producer_partitions,
				..
			} = input
			else {
				panic!("consumer shuffle input");
			};
			assert!(matches!(
				producer_partitions.as_slice(),
				[ShufflePartitionRef::Live(_)]
			));
		}

		dispatcher.release();
		dispatch.await.expect("dispatch");
	}

	async fn assert_multi_upstream_join_keeps_barrier(
		left_tasks: Vec<TaskPlan>,
		right_tasks: Vec<TaskPlan>,
		left_exchange_id: ExchangeId,
		right_exchange_id: ExchangeId,
		message: &str,
	) {
		let blocked_tasks = left_tasks
			.iter()
			.chain(right_tasks.iter())
			.map(|task| TaskKey::from_identity(task.spec.identity))
			.collect::<Vec<_>>();
		let consumer = hash_join_consumer(
			StageId::new(2),
			left_exchange_id,
			StageId::new(0),
			right_exchange_id,
			StageId::new(1),
			JoinKind::Inner,
		);
		let expected_producer_count = left_tasks.len() + right_tasks.len();
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stages: vec![],
				tasks: left_tasks,
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stages: vec![],
				tasks: right_tasks,
			},
			StagePlan {
				stage_id: StageId::new(2),
				upstream_stages: vec![StageId::new(0), StageId::new(1)],
				tasks: vec![consumer],
			},
		];
		let mut scheduler = DistributedScheduler::new(registry());
		let dispatcher = BlockingTaskSetDispatcher::new(blocked_tasks);
		let dispatch = scheduler.dispatch_loop(&stages, &dispatcher);
		tokio::pin!(dispatch);

		tokio::select! {
			seen = tokio::time::timeout(
				std::time::Duration::from_secs(1),
				dispatcher.wait_for_seen(expected_producer_count),
			) => {
				seen.expect("all producers should dispatch");
			}
			result = &mut dispatch => {
				panic!("dispatch completed before blocked producers were observed: {result:?}");
			}
		}
		tokio::select! {
			no_consumer = tokio::time::timeout(
				std::time::Duration::from_millis(50),
				dispatcher.wait_for_seen(expected_producer_count + 1),
			) => {
				no_consumer.expect_err(message);
			}
			result = &mut dispatch => {
				panic!("multi-upstream consumer dispatched before barrier opened: {result:?}");
			}
		}

		dispatcher.release();
		dispatch.await.expect("dispatch");
	}

	#[tokio::test]
	async fn single_stage_query_dispatches_and_accepts() {
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stages: vec![],
			tasks: vec![task(StageId::new(0), TaskId::new(0))],
		};
		let mut scheduler = DistributedScheduler::new(registry());
		let mut dispatcher = ScriptedDispatcher::new([completed()]);

		let run = scheduler
			.dispatch_loop(&[stage], &mut dispatcher)
			.await
			.expect("dispatch");

		assert_eq!(run.dispatched.len(), 1);
		assert!(matches!(
			scheduler
				.state()
				.acceptance_state(TaskKey::from_identity(run.dispatched[0])),
			AcceptanceState::LiveAccepted { .. }
		));
	}

	#[tokio::test]
	async fn dispatch_loop_has_in_flight_frontier() {
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stages: vec![],
			tasks: vec![
				task(StageId::new(0), TaskId::new(0)),
				task(StageId::new(0), TaskId::new(1)),
			],
		};
		let mut scheduler = DistributedScheduler::new(registry());
		let dispatcher = BlockingFirstDispatcher::new();
		let stages = [stage];
		let dispatch = scheduler.dispatch_loop(&stages, &dispatcher);
		tokio::pin!(dispatch);

		tokio::select! {
			seen = tokio::time::timeout(
				std::time::Duration::from_secs(1),
				dispatcher.wait_for_seen(2),
			) => {
				seen.expect("second task should dispatch while first is still running");
			}
			result = &mut dispatch => {
				panic!("dispatch completed before two tasks were in flight: {result:?}");
			}
		}
		assert_eq!(dispatcher.seen().len(), 2);
		dispatcher.release_first();

		let run = dispatch.await.expect("dispatch");
		assert_eq!(run.dispatched.len(), 2);
	}

	#[tokio::test]
	async fn max_in_flight_task_cap_limits_frontier() {
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stages: vec![],
			tasks: vec![
				task(StageId::new(0), TaskId::new(0)),
				task(StageId::new(0), TaskId::new(1)),
				task(StageId::new(0), TaskId::new(2)),
			],
		};
		let mut scheduler = DistributedScheduler::new(registry()).with_max_in_flight_tasks(Some(1));
		let dispatcher = BlockingFirstDispatcher::new();
		let stages = [stage];
		let dispatch = scheduler.dispatch_loop(&stages, &dispatcher);
		tokio::pin!(dispatch);

		tokio::select! {
			seen = tokio::time::timeout(
				std::time::Duration::from_secs(1),
				dispatcher.wait_for_seen(1),
			) => {
				seen.expect("first task should dispatch");
			}
			result = &mut dispatch => {
				panic!("dispatch completed before the capped first task was released: {result:?}");
			}
		}
		tokio::select! {
			no_second = tokio::time::timeout(
				std::time::Duration::from_millis(50),
				dispatcher.wait_for_seen(2),
			) => {
				no_second.expect_err("second task must wait for in-flight capacity");
			}
			result = &mut dispatch => {
				panic!("dispatch completed before the capped first task was released: {result:?}");
			}
		}
		assert_eq!(dispatcher.seen().len(), 1);
		dispatcher.release_first();

		let run = dispatch.await.expect("dispatch");
		assert_eq!(run.dispatched.len(), 3);
	}

	async fn assert_single_exchange_consumer_pipelines_before_producer_completion() {
		let exchange_id = ExchangeId::new(119);
		let producer = streamable_shuffle_producer(StageId::new(0), TaskId::new(0), exchange_id);
		let producer_key = TaskKey::from_identity(producer.spec.identity);
		let consumer = shuffle_consumer(StageId::new(1), exchange_id, StageId::new(0));
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stages: vec![],
				tasks: vec![producer],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stages: vec![StageId::new(0)],
				tasks: vec![consumer],
			},
		];
		let mut scheduler = DistributedScheduler::new(registry());
		let dispatcher = BlockingProducerDispatcher::new(producer_key, exchange_id);
		let dispatch = scheduler.dispatch_loop(&stages, &dispatcher);
		tokio::pin!(dispatch);

		tokio::select! {
			seen = tokio::time::timeout(
				std::time::Duration::from_secs(1),
				dispatcher.wait_for_seen(2),
			) => {
				seen.expect("consumer should dispatch while producer is still running");
			}
			result = &mut dispatch => {
				panic!("dispatch completed before live consumer was observed: {result:?}");
			}
		}
		let seen = dispatcher.seen();
		assert_eq!(seen[0].identity.stage_id, StageId::new(0));
		assert_eq!(seen[1].identity.stage_id, StageId::new(1));
		let TaskInputBinding::ShuffleConsumer {
			producer_partitions,
			..
		} = &seen[1].inputs[0]
		else {
			panic!("consumer shuffle input");
		};
		assert!(matches!(
			producer_partitions.as_slice(),
			[ShufflePartitionRef::Live(_)]
		));

		dispatcher.release();
		dispatch.await.expect("dispatch");
	}

	#[tokio::test]
	async fn single_exchange_consumer_starts_before_producer_task_complete() {
		assert_single_exchange_consumer_pipelines_before_producer_completion().await;
	}

	#[tokio::test]
	async fn barrier_only_revert_would_break_the_pipelining_gate() {
		assert_single_exchange_consumer_pipelines_before_producer_completion().await;
	}

	#[tokio::test]
	async fn multi_upstream_streamable_both_consumer_dispatches_before_either_upstream_accepts() {
		assert_multi_upstream_streamable_join_dispatches_before_acceptance(false).await;
	}

	#[tokio::test]
	async fn multi_upstream_streamable_both_consumer_starts_before_sealed_manifests() {
		assert_multi_upstream_streamable_join_dispatches_before_acceptance(true).await;
	}

	#[tokio::test]
	async fn multi_upstream_non_streamable_one_side_keeps_barrier() {
		let left_exchange_id = ExchangeId::new(1233);
		let right_exchange_id = ExchangeId::new(1234);
		assert_multi_upstream_join_keeps_barrier(
			vec![streamable_shuffle_producer(
				StageId::new(0),
				TaskId::new(0),
				left_exchange_id,
			)],
			vec![non_streamable_shuffle_producer(
				StageId::new(1),
				TaskId::new(0),
				right_exchange_id,
			)],
			left_exchange_id,
			right_exchange_id,
			"one non-streamable upstream must keep the multi-upstream barrier",
		)
		.await;
	}

	#[tokio::test]
	async fn multi_upstream_non_streamable_both_sides_keeps_barrier() {
		let left_exchange_id = ExchangeId::new(1235);
		let right_exchange_id = ExchangeId::new(1236);
		assert_multi_upstream_join_keeps_barrier(
			vec![non_streamable_shuffle_producer(
				StageId::new(0),
				TaskId::new(0),
				left_exchange_id,
			)],
			vec![non_streamable_shuffle_producer(
				StageId::new(1),
				TaskId::new(0),
				right_exchange_id,
			)],
			left_exchange_id,
			right_exchange_id,
			"two non-streamable upstreams must keep the multi-upstream barrier",
		)
		.await;
	}

	#[tokio::test]
	async fn multi_upstream_multi_producer_fan_in_keeps_barrier() {
		let left_exchange_id = ExchangeId::new(1237);
		let right_exchange_id = ExchangeId::new(1238);
		assert_multi_upstream_join_keeps_barrier(
			vec![
				streamable_shuffle_producer(StageId::new(0), TaskId::new(0), left_exchange_id),
				streamable_shuffle_producer(StageId::new(0), TaskId::new(1), left_exchange_id),
			],
			vec![streamable_shuffle_producer(
				StageId::new(1),
				TaskId::new(0),
				right_exchange_id,
			)],
			left_exchange_id,
			right_exchange_id,
			"multi-producer fan-in must keep the multi-upstream barrier",
		)
		.await;
	}

	#[tokio::test]
	async fn multi_upstream_streamable_both_mixed_live_and_sealed_keeps_barrier() {
		let left_exchange_id = ExchangeId::new(1239);
		let right_exchange_id = ExchangeId::new(1240);
		let left = streamable_shuffle_producer(StageId::new(0), TaskId::new(0), left_exchange_id);
		let right = streamable_shuffle_producer(StageId::new(1), TaskId::new(0), right_exchange_id);
		let right_key = TaskKey::from_identity(right.spec.identity);
		let consumer = hash_join_consumer(
			StageId::new(2),
			left_exchange_id,
			StageId::new(0),
			right_exchange_id,
			StageId::new(1),
			JoinKind::Inner,
		);
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stages: vec![],
				tasks: vec![left],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stages: vec![StageId::new(0)],
				tasks: vec![right],
			},
			StagePlan {
				stage_id: StageId::new(2),
				upstream_stages: vec![StageId::new(0), StageId::new(1)],
				tasks: vec![consumer],
			},
		];
		let mut scheduler = DistributedScheduler::new(registry());
		let dispatcher = BlockingTaskSetDispatcher::new([right_key]);
		let dispatch = scheduler.dispatch_loop(&stages, &dispatcher);
		tokio::pin!(dispatch);

		tokio::select! {
			seen = tokio::time::timeout(
				std::time::Duration::from_secs(1),
				dispatcher.wait_for_seen(2),
			) => {
				seen.expect("left should seal before right producer starts");
			}
			result = &mut dispatch => {
				panic!("dispatch completed before blocked right producer was observed: {result:?}");
			}
		}
		tokio::select! {
			no_consumer = tokio::time::timeout(
				std::time::Duration::from_millis(50),
				dispatcher.wait_for_seen(3),
			) => {
				no_consumer.expect_err("mixed sealed/live multi-upstream input is deferred");
			}
			result = &mut dispatch => {
				panic!("mixed sealed/live consumer dispatched before all inputs sealed: {result:?}");
			}
		}

		dispatcher.release();
		dispatch.await.expect("dispatch");
		let seen = dispatcher.seen();
		for input in &seen[2].inputs {
			let TaskInputBinding::ShuffleConsumer {
				producer_partitions,
				..
			} = input
			else {
				panic!("consumer shuffle input");
			};
			assert!(
				producer_partitions
					.iter()
					.all(|partition| matches!(partition, ShufflePartitionRef::Sealed(_)))
			);
		}
	}

	#[tokio::test]
	async fn single_upstream_streamable_path_unchanged() {
		assert_single_exchange_consumer_pipelines_before_producer_completion().await;
	}

	#[tokio::test]
	async fn non_streamable_exchange_falls_back_to_sealed_manifest() {
		let exchange_id = ExchangeId::new(120);
		let producer =
			non_streamable_shuffle_producer(StageId::new(0), TaskId::new(0), exchange_id);
		let producer_key = TaskKey::from_identity(producer.spec.identity);
		let consumer = shuffle_consumer(StageId::new(1), exchange_id, StageId::new(0));
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stages: vec![],
				tasks: vec![producer],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stages: vec![StageId::new(0)],
				tasks: vec![consumer],
			},
		];
		let mut scheduler = DistributedScheduler::new(registry());
		let dispatcher = BlockingProducerDispatcher::new(producer_key, exchange_id);
		let dispatch = scheduler.dispatch_loop(&stages, &dispatcher);
		tokio::pin!(dispatch);

		tokio::select! {
			seen = tokio::time::timeout(
				std::time::Duration::from_secs(1),
				dispatcher.wait_for_seen(1),
			) => {
				seen.expect("producer should dispatch");
			}
			result = &mut dispatch => {
				panic!("dispatch completed before blocked producer was released: {result:?}");
			}
		}
		tokio::select! {
			no_consumer = tokio::time::timeout(
				std::time::Duration::from_millis(50),
				dispatcher.wait_for_seen(2),
			) => {
				no_consumer.expect_err("non-streamable producer must not expose a live consumer path");
			}
			result = &mut dispatch => {
				panic!("non-streamable producer exposed a live consumer path: {result:?}");
			}
		}
		assert_eq!(dispatcher.seen().len(), 1);

		dispatcher.release();
		dispatch.await.expect("dispatch");
		let seen = dispatcher.seen();
		let TaskInputBinding::ShuffleConsumer {
			producer_partitions,
			..
		} = &seen[1].inputs[0]
		else {
			panic!("consumer shuffle input");
		};
		assert!(matches!(
			producer_partitions.as_slice(),
			[ShufflePartitionRef::Sealed(_)]
		));
	}

	#[tokio::test]
	async fn hash_shuffle_full_fan_in_still_waits_for_all_producers() {
		let exchange_id = ExchangeId::new(121);
		let producer_a = streamable_shuffle_producer(StageId::new(0), TaskId::new(0), exchange_id);
		let producer_b = streamable_shuffle_producer(StageId::new(0), TaskId::new(1), exchange_id);
		let producer_b_key = TaskKey::from_identity(producer_b.spec.identity);
		let consumer = shuffle_consumer(StageId::new(1), exchange_id, StageId::new(0));
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stages: vec![],
				tasks: vec![producer_a, producer_b],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stages: vec![StageId::new(0)],
				tasks: vec![consumer],
			},
		];
		let mut scheduler = DistributedScheduler::new(registry());
		let dispatcher = BlockingProducerDispatcher::new(producer_b_key, exchange_id);
		let dispatch = scheduler.dispatch_loop(&stages, &dispatcher);
		tokio::pin!(dispatch);

		tokio::select! {
		seen = tokio::time::timeout(
			std::time::Duration::from_secs(1),
			dispatcher.wait_for_seen(2),
		) => {
			seen.expect("both producers should dispatch");
		}
		result = &mut dispatch => {
			panic!("dispatch completed before blocked producer was released: {result:?}");
		}
		}
		tokio::select! {
			no_consumer = tokio::time::timeout(
				std::time::Duration::from_millis(50),
				dispatcher.wait_for_seen(3),
			) => {
				no_consumer.expect_err("full-fan-in consumer must wait for every producer to seal");
			}
			result = &mut dispatch => {
				panic!("full-fan-in consumer dispatched before every producer sealed: {result:?}");
			}
		}

		dispatcher.release();
		dispatch.await.expect("dispatch");
		let seen = dispatcher.seen();
		let TaskInputBinding::ShuffleConsumer {
			producer_partitions,
			..
		} = &seen[2].inputs[0]
		else {
			panic!("consumer shuffle input");
		};
		assert_eq!(producer_partitions.len(), 2);
		assert!(
			producer_partitions
				.iter()
				.all(|partition| matches!(partition, ShufflePartitionRef::Sealed(_)))
		);
	}

	#[tokio::test]
	async fn consumer_live_refs_cover_all_expected_producers() {
		let exchange_id = ExchangeId::new(122);
		let producer_a = streamable_shuffle_producer(StageId::new(0), TaskId::new(0), exchange_id);
		let producer_b = streamable_shuffle_producer(StageId::new(0), TaskId::new(1), exchange_id);
		let consumer = shuffle_consumer(StageId::new(1), exchange_id, StageId::new(0));
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stages: vec![],
				tasks: vec![producer_a, producer_b],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stages: vec![StageId::new(0)],
				tasks: vec![consumer],
			},
		];
		let mut scheduler = DistributedScheduler::new(registry());
		let never_blocked_key =
			TaskKey::from_identity(task(StageId::new(99), TaskId::new(99)).spec.identity);
		let dispatcher = BlockingProducerDispatcher::new(never_blocked_key, exchange_id);

		scheduler
			.dispatch_loop(&stages, &dispatcher)
			.await
			.expect("dispatch");

		let seen = dispatcher.seen();
		let TaskInputBinding::ShuffleConsumer {
			producer_partitions,
			..
		} = &seen[2].inputs[0]
		else {
			panic!("consumer shuffle input");
		};
		assert_eq!(producer_partitions.len(), 2);
		assert_eq!(
			producer_partitions
				.iter()
				.filter_map(|partition| match partition {
					ShufflePartitionRef::Sealed(sealed) => Some(sealed.address.producer_task_id),
					ShufflePartitionRef::Live(_) => None,
				})
				.collect::<Vec<_>>(),
			vec![TaskId::new(0), TaskId::new(1)]
		);
	}

	#[tokio::test]
	async fn stage_barrier_dispatches_consumer_after_upstream_acceptance() {
		let producer = task(StageId::new(0), TaskId::new(0));
		let consumer = task(StageId::new(1), TaskId::new(0));
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stages: vec![],
				tasks: vec![producer],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stages: vec![StageId::new(0)],
				tasks: vec![consumer],
			},
		];
		let mut scheduler = DistributedScheduler::new(registry());
		let mut dispatcher = ScriptedDispatcher::new([completed(), completed()]);

		let run = scheduler
			.dispatch_loop(&stages, &mut dispatcher)
			.await
			.expect("dispatch");

		assert_eq!(run.dispatched[0].stage_id, StageId::new(0));
		assert_eq!(run.dispatched[1].stage_id, StageId::new(1));
	}

	#[tokio::test]
	async fn stage_barrier_waits_for_both_upstreams() {
		let left = task(StageId::new(0), TaskId::new(0));
		let right = task(StageId::new(1), TaskId::new(0));
		let consumer = task(StageId::new(2), TaskId::new(0));
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stages: vec![],
				tasks: vec![left],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stages: vec![],
				tasks: vec![right],
			},
			StagePlan {
				stage_id: StageId::new(2),
				upstream_stages: vec![StageId::new(0), StageId::new(1)],
				tasks: vec![consumer],
			},
		];
		let mut scheduler = DistributedScheduler::new(registry());
		let mut dispatcher = ScriptedDispatcher::new([completed(), completed(), completed()]);

		let run = scheduler
			.dispatch_loop(&stages, &mut dispatcher)
			.await
			.expect("dispatch");

		assert_eq!(run.dispatched[0].stage_id, StageId::new(0));
		assert_eq!(run.dispatched[1].stage_id, StageId::new(1));
		assert_eq!(run.dispatched[2].stage_id, StageId::new(2));
	}

	#[tokio::test]
	async fn multi_upstream_stage_keeps_stage_barrier() {
		let left = task(StageId::new(0), TaskId::new(0));
		let right = task(StageId::new(1), TaskId::new(0));
		let consumer = task(StageId::new(2), TaskId::new(0));
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stages: vec![],
				tasks: vec![left],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stages: vec![],
				tasks: vec![right],
			},
			StagePlan {
				stage_id: StageId::new(2),
				upstream_stages: vec![StageId::new(0), StageId::new(1)],
				tasks: vec![consumer],
			},
		];
		let mut scheduler = DistributedScheduler::new(registry());
		let dispatcher = BlockingFirstDispatcher::new();
		let dispatch = scheduler.dispatch_loop(&stages, &dispatcher);
		tokio::pin!(dispatch);

		tokio::select! {
			seen = tokio::time::timeout(
				std::time::Duration::from_secs(1),
				dispatcher.wait_for_seen(2),
			) => {
				seen.expect("both upstream tasks should dispatch");
			}
			result = &mut dispatch => {
				panic!("dispatch completed before blocked upstreams were released: {result:?}");
			}
		}
		tokio::select! {
			no_consumer = tokio::time::timeout(
				std::time::Duration::from_millis(50),
				dispatcher.wait_for_seen(3),
			) => {
				no_consumer.expect_err("multi-upstream stage must keep the stage barrier");
			}
			result = &mut dispatch => {
				panic!("multi-upstream consumer dispatched before both upstreams completed: {result:?}");
			}
		}

		tokio::task::yield_now().await;
		dispatcher.release_first();
		let completed_run = tokio::select! {
			seen = tokio::time::timeout(
				std::time::Duration::from_secs(1),
				dispatcher.wait_for_seen(3),
			) => {
				seen.expect("consumer should dispatch after both upstreams complete");
				None
			}
			result = &mut dispatch => {
				let run = result.expect("dispatch");
				assert_eq!(dispatcher.seen().len(), 3);
				Some(run)
			}
		};
		let run = match completed_run {
			Some(run) => run,
			None => dispatch.await.expect("dispatch"),
		};
		assert_eq!(run.dispatched.len(), 3);
		assert_eq!(run.dispatched[2].stage_id, StageId::new(2));
	}

	#[tokio::test]
	async fn retryable_failure_retries_once_then_accepts() {
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stages: vec![],
			tasks: vec![task(StageId::new(0), TaskId::new(0))],
		};
		let mut scheduler = DistributedScheduler::new(registry());
		let mut dispatcher = ScriptedDispatcher::new([
			DispatchOutcome::Failed(TaskFailureKind::TaskFailed {
				reason: "transient".to_string(),
				retryable: true,
			}),
			completed(),
		]);

		let run = scheduler
			.dispatch_loop(&[stage], &mut dispatcher)
			.await
			.expect("dispatch");

		assert_eq!(run.dispatched.len(), 2);
		assert_eq!(run.dispatched[0].attempt_id, AttemptId::first());
		assert_eq!(run.dispatched[1].attempt_id, AttemptId::new(1));
		assert_eq!(run.telemetry.retry_count, 1);
	}

	#[tokio::test]
	async fn completed_task_telemetry_is_collected() {
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stages: vec![],
			tasks: vec![task(StageId::new(0), TaskId::new(0))],
		};
		let mut telemetry = TaskTelemetry::default();
		telemetry.labels.operator_kind = "join".to_string();
		telemetry.counters.pulled_shuffle_bytes = 128;
		let mut scheduler = DistributedScheduler::new(registry());
		let mut dispatcher = ScriptedDispatcher::new([DispatchOutcome::CompletedWithTelemetry {
			output: TaskCompletionOutput::FinalResult {
				output_digest: None,
			},
			batches: Vec::new(),
			telemetry: telemetry.clone(),
		}]);

		let run = scheduler
			.dispatch_loop(&[stage], &mut dispatcher)
			.await
			.expect("dispatch");

		assert_eq!(run.telemetry.task_telemetry.len(), 1);
		let collected = &run.telemetry.task_telemetry[0];
		assert_eq!(collected.labels.operator_kind, "join");
		assert_eq!(collected.counters.pulled_shuffle_bytes, 128);
		assert!(collected.phases.dispatch_wait_micros > 0);
	}

	#[tokio::test]
	async fn max_attempts_failure_marks_acceptance_failed() {
		let task = task(StageId::new(0), TaskId::new(0));
		let key = TaskKey::from_identity(task.spec.identity);
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stages: vec![],
			tasks: vec![task],
		};
		let mut scheduler = DistributedScheduler::new(registry());
		let kind = TaskFailureKind::TaskFailed {
			reason: "still failing".to_string(),
			retryable: true,
		};
		let mut dispatcher = ScriptedDispatcher::new([
			DispatchOutcome::Failed(kind.clone()),
			DispatchOutcome::Failed(kind.clone()),
		]);

		let err = scheduler
			.dispatch_loop(&[stage], &mut dispatcher)
			.await
			.expect_err("max attempts fail");

		assert_eq!(
			err,
			SchedulerError::TaskFailed {
				key,
				kind: kind.clone(),
			}
		);
		assert_eq!(
			scheduler.state().acceptance_state(key),
			AcceptanceState::Failed { kind }
		);
	}

	#[tokio::test]
	async fn completed_shuffle_manifest_is_recorded() {
		let mut producer = task(StageId::new(0), TaskId::new(0));
		producer.spec.output = TaskOutputBinding::ShuffleProducer {
			exchange_id: ExchangeId::new(9),
			partition_count: crate::distributed::task::PartitionCount::new(1),
			partition_key: BoundExprProgram::empty(),
			publish_mode: crate::distributed::task::ShufflePublishMode::Live,
		};
		let key = TaskKey::from_identity(producer.spec.identity);
		let manifest = PartitionManifest {
			query_id: producer.spec.identity.query_id,
			stage_id: producer.spec.identity.stage_id,
			task_id: producer.spec.identity.task_id,
			attempt_id: producer.spec.identity.attempt_id,
			exchange_id: ExchangeId::new(9),
			producer_worker_id: WorkerId::from_u128(1),
			producer_endpoint: WorkerEndpoint::new("127.0.0.1:0"),
			partitions: vec![PartitionEntry {
				partition_id: PartitionId::new(0),
				size_bytes: ByteCount::new(1),
				row_count: RowCount::new(1),
				digest: ArrowIpcDigest::new([3; 32]),
			}],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		};
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stages: vec![],
			tasks: vec![producer],
		};
		let mut scheduler = DistributedScheduler::new(registry());
		let mut dispatcher = ScriptedDispatcher::new([DispatchOutcome::Completed(
			TaskCompletionOutput::ShuffleProducer {
				manifest: manifest.clone(),
			},
		)]);

		let run = scheduler
			.dispatch_loop(&[stage], &mut dispatcher)
			.await
			.expect("dispatch");

		assert_eq!(run.accepted_manifests.get(&key), Some(&manifest));
		assert_eq!(scheduler.accepted_manifest(key), Some(&manifest));
	}

	#[tokio::test]
	async fn coordinator_injects_merged_filter_into_probe_task_binding() {
		let build_exchange_id = ExchangeId::new(9);
		let probe_exchange_id = ExchangeId::new(10);
		let mut build = task(StageId::new(0), TaskId::new(0));
		build.spec.output = TaskOutputBinding::ShuffleProducer {
			exchange_id: build_exchange_id,
			partition_count: crate::distributed::task::PartitionCount::new(1),
			partition_key: int64_partition_key(),
			publish_mode: crate::distributed::task::ShufflePublishMode::Live,
		};
		let build_key = TaskKey::from_identity(build.spec.identity);
		let mut probe = task(StageId::new(1), TaskId::new(0));
		probe.spec.output = TaskOutputBinding::ShuffleProducer {
			exchange_id: probe_exchange_id,
			partition_count: crate::distributed::task::PartitionCount::new(1),
			partition_key: int64_partition_key(),
			publish_mode: crate::distributed::task::ShufflePublishMode::Live,
		};
		let build_manifest = PartitionManifest {
			query_id: build.spec.identity.query_id,
			stage_id: build.spec.identity.stage_id,
			task_id: build.spec.identity.task_id,
			attempt_id: build.spec.identity.attempt_id,
			exchange_id: build_exchange_id,
			producer_worker_id: WorkerId::from_u128(1),
			producer_endpoint: WorkerEndpoint::new("127.0.0.1:9191"),
			partitions: vec![PartitionEntry {
				partition_id: PartitionId::new(0),
				size_bytes: ByteCount::new(8),
				row_count: RowCount::new(2),
				digest: ArrowIpcDigest::new([4; 32]),
			}],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		};
		let probe_manifest = PartitionManifest {
			query_id: probe.spec.identity.query_id,
			stage_id: probe.spec.identity.stage_id,
			task_id: probe.spec.identity.task_id,
			attempt_id: probe.spec.identity.attempt_id,
			exchange_id: probe_exchange_id,
			producer_worker_id: WorkerId::from_u128(1),
			producer_endpoint: WorkerEndpoint::new("127.0.0.1:9191"),
			partitions: vec![PartitionEntry {
				partition_id: PartitionId::new(0),
				size_bytes: ByteCount::new(8),
				row_count: RowCount::new(2),
				digest: ArrowIpcDigest::new([5; 32]),
			}],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		};
		let build_batch = RecordBatch::try_new(
			Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
			vec![Arc::new(Int64Array::from(vec![10_i64, 20])) as ArrayRef],
		)
		.expect("build batch");
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stages: Vec::new(),
				tasks: vec![build],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stages: vec![StageId::new(0)],
				tasks: vec![probe],
			},
		];
		let mut scheduler = DistributedScheduler::new(registry());
		let mut dispatcher = ScriptedDispatcher::new([
			DispatchOutcome::CompletedWithBatches {
				output: TaskCompletionOutput::ShuffleProducer {
					manifest: build_manifest,
				},
				batches: vec![build_batch],
			},
			DispatchOutcome::Completed(TaskCompletionOutput::ShuffleProducer {
				manifest: probe_manifest,
			}),
		]);

		let run = scheduler
			.dispatch_loop(&stages, &mut dispatcher)
			.await
			.expect("dispatch");

		let seen = dispatcher.seen();
		let TaskInputBinding::Scan {
			runtime_filter: Some(runtime_filter),
			..
		} = &seen[1].inputs[0]
		else {
			panic!("probe scan binding should carry runtime filter");
		};
		let accepted_build = run
			.accepted_manifests
			.get(&build_key)
			.expect("accepted build manifest");
		let fragment = accepted_build
			.runtime_filter_fragment
			.as_ref()
			.expect("build fragment");
		assert_eq!(runtime_filter.probe_key_columns, vec![0]);
		assert_eq!(
			runtime_filter.probe_descriptor_fingerprint,
			physical_fingerprint()
		);
		assert_eq!(&runtime_filter.kind, &fragment.kind);
	}

	#[tokio::test]
	async fn coordinator_merge_excludes_retryable_failed_attempt() {
		let build_exchange_id = ExchangeId::new(19);
		let probe_exchange_id = ExchangeId::new(20);
		let mut build = task(StageId::new(0), TaskId::new(0));
		build.spec.output = TaskOutputBinding::ShuffleProducer {
			exchange_id: build_exchange_id,
			partition_count: crate::distributed::task::PartitionCount::new(1),
			partition_key: int64_partition_key(),
			publish_mode: crate::distributed::task::ShufflePublishMode::Live,
		};
		let mut probe = task(StageId::new(1), TaskId::new(0));
		probe.spec.output = TaskOutputBinding::ShuffleProducer {
			exchange_id: probe_exchange_id,
			partition_count: crate::distributed::task::PartitionCount::new(1),
			partition_key: int64_partition_key(),
			publish_mode: crate::distributed::task::ShufflePublishMode::Live,
		};
		let build_manifest = PartitionManifest {
			query_id: build.spec.identity.query_id,
			stage_id: build.spec.identity.stage_id,
			task_id: build.spec.identity.task_id,
			attempt_id: AttemptId::new(1),
			exchange_id: build_exchange_id,
			producer_worker_id: WorkerId::from_u128(1),
			producer_endpoint: WorkerEndpoint::new("127.0.0.1:9191"),
			partitions: vec![PartitionEntry {
				partition_id: PartitionId::new(0),
				size_bytes: ByteCount::new(8),
				row_count: RowCount::new(1),
				digest: ArrowIpcDigest::new([6; 32]),
			}],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		};
		let probe_manifest = PartitionManifest {
			query_id: probe.spec.identity.query_id,
			stage_id: probe.spec.identity.stage_id,
			task_id: probe.spec.identity.task_id,
			attempt_id: probe.spec.identity.attempt_id,
			exchange_id: probe_exchange_id,
			producer_worker_id: WorkerId::from_u128(1),
			producer_endpoint: WorkerEndpoint::new("127.0.0.1:9191"),
			partitions: vec![PartitionEntry {
				partition_id: PartitionId::new(0),
				size_bytes: ByteCount::new(8),
				row_count: RowCount::new(1),
				digest: ArrowIpcDigest::new([7; 32]),
			}],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		};
		let accepted_batch = RecordBatch::try_new(
			Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
			vec![Arc::new(Int64Array::from(vec![111_i64])) as ArrayRef],
		)
		.expect("accepted batch");
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stages: Vec::new(),
				tasks: vec![build],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stages: vec![StageId::new(0)],
				tasks: vec![probe],
			},
		];
		let mut scheduler = DistributedScheduler::new(registry());
		let mut dispatcher = ScriptedDispatcher::new([
			DispatchOutcome::Failed(TaskFailureKind::TaskFailed {
				reason: "transient".to_string(),
				retryable: true,
			}),
			DispatchOutcome::CompletedWithBatches {
				output: TaskCompletionOutput::ShuffleProducer {
					manifest: build_manifest,
				},
				batches: vec![accepted_batch],
			},
			DispatchOutcome::Completed(TaskCompletionOutput::ShuffleProducer {
				manifest: probe_manifest,
			}),
		]);

		scheduler
			.dispatch_loop(&stages, &mut dispatcher)
			.await
			.expect("dispatch");

		let seen = dispatcher.seen();
		assert_eq!(seen[0].identity.attempt_id, AttemptId::first());
		assert_eq!(seen[1].identity.attempt_id, AttemptId::new(1));
		let TaskInputBinding::Scan {
			runtime_filter: Some(runtime_filter),
			..
		} = &seen[2].inputs[0]
		else {
			panic!("probe scan binding should carry retry fragment");
		};
		let accepted_fragment = build_runtime_filter_fragment(
			&[RecordBatch::try_new(
				Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
				vec![Arc::new(Int64Array::from(vec![111_i64])) as ArrayRef],
			)
			.expect("expected fragment batch")],
			&int64_partition_key(),
		)
		.expect("fragment result")
		.expect("fragment");
		assert_eq!(&runtime_filter.kind, &accepted_fragment.kind);
	}

	#[test]
	fn coordinator_merge_order_invariant() {
		let query_id = QueryId::from_u128(99);
		let stage_id = StageId::new(3);
		let fragment_a = runtime_filter_fragment(vec![0b0000_1111, 0], 2);
		let fragment_b = runtime_filter_fragment(vec![0, 0b1111_0000], 2);
		let mut left = DistributedScheduler::new(registry());
		left.record_runtime_filter_fragment(query_id, stage_id, fragment_a.clone());
		left.record_runtime_filter_fragment(query_id, stage_id, fragment_b.clone());
		let mut right = DistributedScheduler::new(registry());
		right.record_runtime_filter_fragment(query_id, stage_id, fragment_b);
		right.record_runtime_filter_fragment(query_id, stage_id, fragment_a);

		assert_eq!(
			left.runtime_filter_merges.get(&(query_id, stage_id)),
			right.runtime_filter_merges.get(&(query_id, stage_id))
		);
		assert_eq!(
			left.runtime_filter_merges.get(&(query_id, stage_id)),
			Some(&RuntimeFilterMerge::Active(runtime_filter_fragment(
				vec![0b0000_1111, 0b1111_0000],
				2
			)))
		);
	}

	#[test]
	fn coordinator_merge_drops_incompatible_fragments() {
		let query_id = QueryId::from_u128(100);
		let stage_id = StageId::new(4);
		let mut scheduler = DistributedScheduler::new(registry());

		scheduler.record_runtime_filter_fragment(
			query_id,
			stage_id,
			runtime_filter_fragment(vec![0b0000_1111, 0], 2),
		);
		scheduler.record_runtime_filter_fragment(
			query_id,
			stage_id,
			runtime_filter_fragment(vec![0, 0b1111_0000], 3),
		);

		assert_eq!(
			scheduler.runtime_filter_merges.get(&(query_id, stage_id)),
			Some(&RuntimeFilterMerge::Dropped)
		);
	}

	#[tokio::test]
	async fn cancel_query_removes_manifests_and_closes_tasks() {
		let mut producer = task(StageId::new(0), TaskId::new(0));
		producer.spec.output = TaskOutputBinding::ShuffleProducer {
			exchange_id: ExchangeId::new(9),
			partition_count: crate::distributed::task::PartitionCount::new(1),
			partition_key: BoundExprProgram::empty(),
			publish_mode: crate::distributed::task::ShufflePublishMode::Live,
		};
		let query_id = producer.spec.identity.query_id;
		let key = TaskKey::from_identity(producer.spec.identity);
		let manifest = PartitionManifest {
			query_id,
			stage_id: producer.spec.identity.stage_id,
			task_id: producer.spec.identity.task_id,
			attempt_id: producer.spec.identity.attempt_id,
			exchange_id: ExchangeId::new(9),
			producer_worker_id: WorkerId::from_u128(1),
			producer_endpoint: WorkerEndpoint::new("127.0.0.1:0"),
			partitions: vec![PartitionEntry {
				partition_id: PartitionId::new(0),
				size_bytes: ByteCount::new(1),
				row_count: RowCount::new(1),
				digest: ArrowIpcDigest::new([3; 32]),
			}],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		};
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stages: vec![],
			tasks: vec![producer],
		};
		let mut scheduler = DistributedScheduler::new(registry());
		let mut dispatcher = ScriptedDispatcher::new([DispatchOutcome::Completed(
			TaskCompletionOutput::ShuffleProducer { manifest },
		)]);
		scheduler
			.dispatch_loop(&[stage], &mut dispatcher)
			.await
			.expect("dispatch");

		let cancelled = scheduler.cancel_query(query_id);

		assert_eq!(cancelled, CancelledTaskCount::new(1));
		assert_eq!(scheduler.telemetry.cancellation_cleanup_count, 1);
		assert_eq!(scheduler.accepted_manifest(key), None);
		assert_eq!(
			scheduler.state().acceptance_state(key),
			AcceptanceState::Cancelled
		);
	}

	#[tokio::test]
	async fn worker_loss_removes_accepted_manifest_and_records_replacement_attempt() {
		let worker_id = WorkerId::from_u128(1);
		let mut producer = task(StageId::new(0), TaskId::new(0));
		producer.spec.output = TaskOutputBinding::ShuffleProducer {
			exchange_id: ExchangeId::new(9),
			partition_count: crate::distributed::task::PartitionCount::new(1),
			partition_key: BoundExprProgram::empty(),
			publish_mode: crate::distributed::task::ShufflePublishMode::Live,
		};
		let key = TaskKey::from_identity(producer.spec.identity);
		let manifest = PartitionManifest {
			query_id: producer.spec.identity.query_id,
			stage_id: producer.spec.identity.stage_id,
			task_id: producer.spec.identity.task_id,
			attempt_id: producer.spec.identity.attempt_id,
			exchange_id: ExchangeId::new(9),
			producer_worker_id: worker_id,
			producer_endpoint: WorkerEndpoint::new("127.0.0.1:0"),
			partitions: vec![PartitionEntry {
				partition_id: PartitionId::new(0),
				size_bytes: ByteCount::new(1),
				row_count: RowCount::new(1),
				digest: ArrowIpcDigest::new([3; 32]),
			}],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		};
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stages: vec![],
			tasks: vec![producer],
		};
		let mut scheduler = DistributedScheduler::new(registry());
		let mut dispatcher = ScriptedDispatcher::new([DispatchOutcome::Completed(
			TaskCompletionOutput::ShuffleProducer { manifest },
		)]);
		scheduler
			.dispatch_loop(&[stage], &mut dispatcher)
			.await
			.expect("dispatch");

		let invalidated = scheduler.mark_worker_lost(worker_id);

		assert_eq!(invalidated.producers.len(), 1);
		assert!(invalidated.consumers.is_empty());
		assert_eq!(invalidated.producers[0].key, key);
		assert_eq!(
			invalidated.producers[0].invalidated_attempt,
			AttemptId::first()
		);
		assert_eq!(
			invalidated.producers[0].replacement_attempt,
			AttemptId::new(1)
		);
		assert_eq!(scheduler.accepted_manifest(key), None);
		assert_eq!(
			scheduler.state().acceptance_state(key),
			AcceptanceState::Invalidated {
				attempt_id: AttemptId::first(),
				worker_id,
			}
		);
		assert_eq!(
			scheduler.state().pending_attempt(key),
			Some(key.attempt(AttemptId::new(1)))
		);
	}

	#[tokio::test]
	async fn heartbeat_expiry_invalidates_accepted_manifest() {
		let worker_id = WorkerId::from_u128(1);
		let mut producer = task(StageId::new(0), TaskId::new(0));
		producer.spec.output = TaskOutputBinding::ShuffleProducer {
			exchange_id: ExchangeId::new(9),
			partition_count: crate::distributed::task::PartitionCount::new(1),
			partition_key: BoundExprProgram::empty(),
			publish_mode: crate::distributed::task::ShufflePublishMode::Live,
		};
		let key = TaskKey::from_identity(producer.spec.identity);
		let manifest = PartitionManifest {
			query_id: producer.spec.identity.query_id,
			stage_id: producer.spec.identity.stage_id,
			task_id: producer.spec.identity.task_id,
			attempt_id: producer.spec.identity.attempt_id,
			exchange_id: ExchangeId::new(9),
			producer_worker_id: worker_id,
			producer_endpoint: WorkerEndpoint::new("127.0.0.1:0"),
			partitions: vec![PartitionEntry {
				partition_id: PartitionId::new(0),
				size_bytes: ByteCount::new(1),
				row_count: RowCount::new(1),
				digest: ArrowIpcDigest::new([3; 32]),
			}],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		};
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stages: vec![],
			tasks: vec![producer],
		};
		let registry = Arc::new(StaticWorkerRegistry::new([worker_entry(
			worker_id,
			ActiveTaskCount::default(),
		)]));
		let mut scheduler = DistributedScheduler::new(registry.clone());
		let mut dispatcher = ScriptedDispatcher::new([DispatchOutcome::Completed(
			TaskCompletionOutput::ShuffleProducer { manifest },
		)]);
		scheduler
			.dispatch_loop(&[stage], &mut dispatcher)
			.await
			.expect("dispatch");

		let invalidations = scheduler.expire_worker_heartbeats(
			UnixMillis::new(600),
			crate::distributed::budget::DurationMillis::new(100),
			crate::distributed::budget::DurationMillis::new(500),
		);

		assert_eq!(invalidations.len(), 1);
		assert_eq!(invalidations[0].producers.len(), 1);
		assert_eq!(scheduler.accepted_manifest(key), None);
		assert_eq!(
			scheduler.state().pending_attempt(key),
			Some(key.attempt(AttemptId::new(1)))
		);
		assert_eq!(
			registry.resolve(worker_id).expect("worker").lease.epoch,
			crate::distributed::identity::LeaseEpoch::new(2)
		);
	}

	#[test]
	fn worker_loss_cascades_to_consumers_referencing_invalidated_manifest() {
		let producer_worker_id = WorkerId::from_u128(1);
		let consumer_worker_id = WorkerId::from_u128(2);
		let exchange_id = ExchangeId::new(9);
		let partition_id = PartitionId::new(0);
		let producer_identity = TaskIdentity::new(
			QueryId::from_u128(10),
			StageId::new(0),
			TaskId::new(0),
			AttemptId::first(),
		);
		let consumer_identity = TaskIdentity::new(
			QueryId::from_u128(10),
			StageId::new(1),
			TaskId::new(0),
			AttemptId::first(),
		);
		let producer_key = TaskKey::from_identity(producer_identity);
		let consumer_key = TaskKey::from_identity(consumer_identity);
		let address = PartitionAddress {
			query_id: producer_identity.query_id,
			exchange_id,
			producer_stage_id: producer_identity.stage_id,
			producer_task_id: producer_identity.task_id,
			attempt_id: producer_identity.attempt_id,
			partition_id,
		};
		let manifest = PartitionManifest {
			query_id: producer_identity.query_id,
			stage_id: producer_identity.stage_id,
			task_id: producer_identity.task_id,
			attempt_id: producer_identity.attempt_id,
			exchange_id,
			producer_worker_id,
			producer_endpoint: WorkerEndpoint::new("127.0.0.1:0"),
			partitions: vec![PartitionEntry {
				partition_id,
				size_bytes: ByteCount::new(1),
				row_count: RowCount::new(1),
				digest: ArrowIpcDigest::new([3; 32]),
			}],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		};
		let mut scheduler = DistributedScheduler::new(registry());
		scheduler.state.spawn_attempt(producer_identity);
		scheduler.state.set_attempt_state(
			producer_identity,
			AttemptState::Completed {
				worker_id: producer_worker_id,
			},
		);
		scheduler
			.state
			.accept_completed(producer_identity, producer_worker_id)
			.expect("accept producer");
		scheduler.state.spawn_attempt(consumer_identity);
		scheduler.state.set_attempt_state(
			consumer_identity,
			AttemptState::Completed {
				worker_id: consumer_worker_id,
			},
		);
		scheduler
			.state
			.accept_completed(consumer_identity, consumer_worker_id)
			.expect("accept consumer");
		scheduler.accepted_manifests.insert(producer_key, manifest);
		scheduler
			.task_dependencies
			.insert(consumer_key, vec![address]);

		let invalidated = scheduler.mark_worker_lost(producer_worker_id);

		assert_eq!(invalidated.producers.len(), 1);
		assert_eq!(invalidated.consumers.len(), 1);
		assert_eq!(invalidated.producers[0].key, producer_key);
		assert_eq!(invalidated.consumers[0].key, consumer_key);
		assert_eq!(scheduler.accepted_manifest(producer_key), None);
		assert_eq!(
			scheduler.state().pending_attempt(producer_key),
			Some(producer_key.attempt(AttemptId::new(1)))
		);
		assert_eq!(
			scheduler.state().pending_attempt(consumer_key),
			Some(consumer_key.attempt(AttemptId::new(1)))
		);
		assert!(!scheduler.task_dependencies.contains_key(&consumer_key));
	}

	#[tokio::test]
	async fn redispatch_after_worker_loss_uses_replacement_attempt_id() {
		let lost_worker_id = WorkerId::from_u128(1);
		let replacement_worker_id = WorkerId::from_u128(2);
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stages: vec![],
			tasks: vec![task(StageId::new(0), TaskId::new(0))],
		};
		let key = TaskKey::from_identity(stage.tasks[0].spec.identity);
		let mut scheduler = DistributedScheduler::new(failover_registry());
		let mut first_dispatch = ScriptedDispatcher::new([completed()]);
		scheduler
			.dispatch_loop(std::slice::from_ref(&stage), &mut first_dispatch)
			.await
			.expect("first dispatch");
		scheduler.mark_worker_lost(lost_worker_id);
		let mut second_dispatch = ScriptedDispatcher::new([completed()]);

		let run = scheduler
			.dispatch_loop(&[stage], &mut second_dispatch)
			.await
			.expect("redispatch");

		assert_eq!(run.dispatched, vec![key.attempt(AttemptId::new(1))]);
		assert_eq!(
			scheduler.state().acceptance_state(key),
			AcceptanceState::LiveAccepted {
				attempt_id: AttemptId::new(1),
				worker_id: replacement_worker_id,
				drained: false,
			}
		);
	}

	#[tokio::test]
	async fn consumer_specs_materialize_partition_refs_from_accepted_manifests() {
		let exchange_id = ExchangeId::new(9);
		let partition_id = PartitionId::new(0);
		let mut producer = task(StageId::new(0), TaskId::new(0));
		producer.spec.output = TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count: crate::distributed::task::PartitionCount::new(1),
			partition_key: BoundExprProgram::empty(),
			publish_mode: crate::distributed::task::ShufflePublishMode::Live,
		};
		let mut consumer = task(StageId::new(1), TaskId::new(0));
		consumer.spec.inputs = vec![TaskInputBinding::ShuffleConsumer {
			exchange_id,
			producer_stage_id: StageId::new(0),
			partition_id,
			producer_partitions: Vec::new(),
		}];
		let endpoint = WorkerEndpoint::new("127.0.0.1:9191");
		let manifest = PartitionManifest {
			query_id: producer.spec.identity.query_id,
			stage_id: producer.spec.identity.stage_id,
			task_id: producer.spec.identity.task_id,
			attempt_id: producer.spec.identity.attempt_id,
			exchange_id,
			producer_worker_id: WorkerId::from_u128(1),
			producer_endpoint: endpoint.clone(),
			partitions: vec![PartitionEntry {
				partition_id,
				size_bytes: ByteCount::new(8),
				row_count: RowCount::new(1),
				digest: ArrowIpcDigest::new([4; 32]),
			}],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		};
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stages: vec![],
				tasks: vec![producer],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stages: vec![StageId::new(0)],
				tasks: vec![consumer],
			},
		];
		let mut scheduler = DistributedScheduler::new(registry());
		let mut dispatcher = ScriptedDispatcher::new([
			DispatchOutcome::Completed(TaskCompletionOutput::ShuffleProducer {
				manifest: manifest.clone(),
			}),
			completed(),
		]);

		scheduler
			.dispatch_loop(&stages, &mut dispatcher)
			.await
			.expect("dispatch");

		let seen = dispatcher.seen();
		let TaskInputBinding::ShuffleConsumer {
			producer_partitions,
			..
		} = &seen[1].inputs[0]
		else {
			panic!("consumer input should remain a shuffle consumer");
		};
		assert_eq!(producer_partitions.len(), 1);
		assert_eq!(producer_partitions[0].endpoint(), &endpoint);
		assert_eq!(producer_partitions[0].address().partition_id, partition_id);
	}

	#[tokio::test]
	async fn producer_is_marked_drained_after_downstream_acceptance() {
		let exchange_id = ExchangeId::new(9);
		let partition_id = PartitionId::new(0);
		let mut producer = task(StageId::new(0), TaskId::new(0));
		producer.spec.output = TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count: crate::distributed::task::PartitionCount::new(1),
			partition_key: BoundExprProgram::empty(),
			publish_mode: crate::distributed::task::ShufflePublishMode::Live,
		};
		let producer_key = TaskKey::from_identity(producer.spec.identity);
		let mut consumer = task(StageId::new(1), TaskId::new(0));
		consumer.spec.inputs = vec![TaskInputBinding::ShuffleConsumer {
			exchange_id,
			producer_stage_id: StageId::new(0),
			partition_id,
			producer_partitions: Vec::new(),
		}];
		let manifest = PartitionManifest {
			query_id: producer.spec.identity.query_id,
			stage_id: producer.spec.identity.stage_id,
			task_id: producer.spec.identity.task_id,
			attempt_id: producer.spec.identity.attempt_id,
			exchange_id,
			producer_worker_id: WorkerId::from_u128(1),
			producer_endpoint: WorkerEndpoint::new("127.0.0.1:9191"),
			partitions: vec![PartitionEntry {
				partition_id,
				size_bytes: ByteCount::new(8),
				row_count: RowCount::new(1),
				digest: ArrowIpcDigest::new([4; 32]),
			}],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		};
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stages: vec![],
				tasks: vec![producer],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stages: vec![StageId::new(0)],
				tasks: vec![consumer],
			},
		];
		let mut scheduler = DistributedScheduler::new(registry());
		let mut dispatcher = ScriptedDispatcher::new([
			DispatchOutcome::Completed(TaskCompletionOutput::ShuffleProducer { manifest }),
			completed(),
		]);

		scheduler
			.dispatch_loop(&stages, &mut dispatcher)
			.await
			.expect("dispatch");

		assert_eq!(
			scheduler.state().acceptance_state(producer_key),
			AcceptanceState::LiveAccepted {
				attempt_id: AttemptId::first(),
				worker_id: WorkerId::from_u128(1),
				drained: true,
			}
		);
	}

	#[tokio::test]
	async fn consumer_exchange_id_mismatch_is_rejected_before_dispatch() {
		let manifest_exchange_id = ExchangeId::new(9);
		let consumer_exchange_id = ExchangeId::new(10);
		let partition_id = PartitionId::new(0);
		let mut producer = task(StageId::new(0), TaskId::new(0));
		producer.spec.output = TaskOutputBinding::ShuffleProducer {
			exchange_id: manifest_exchange_id,
			partition_count: crate::distributed::task::PartitionCount::new(1),
			partition_key: BoundExprProgram::empty(),
			publish_mode: crate::distributed::task::ShufflePublishMode::Live,
		};
		let mut consumer = task(StageId::new(1), TaskId::new(0));
		consumer.spec.inputs = vec![TaskInputBinding::ShuffleConsumer {
			exchange_id: consumer_exchange_id,
			producer_stage_id: StageId::new(0),
			partition_id,
			producer_partitions: Vec::new(),
		}];
		let manifest = PartitionManifest {
			query_id: producer.spec.identity.query_id,
			stage_id: producer.spec.identity.stage_id,
			task_id: producer.spec.identity.task_id,
			attempt_id: producer.spec.identity.attempt_id,
			exchange_id: manifest_exchange_id,
			producer_worker_id: WorkerId::from_u128(1),
			producer_endpoint: WorkerEndpoint::new("127.0.0.1:9191"),
			partitions: vec![PartitionEntry {
				partition_id,
				size_bytes: ByteCount::new(8),
				row_count: RowCount::new(1),
				digest: ArrowIpcDigest::new([4; 32]),
			}],
			runtime_filter_fragment: None,
			backend: PartitionBackend::InMemory,
		};
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stages: vec![],
				tasks: vec![producer],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stages: vec![StageId::new(0)],
				tasks: vec![consumer],
			},
		];
		let mut scheduler = DistributedScheduler::new(registry());
		let mut dispatcher = ScriptedDispatcher::new([
			DispatchOutcome::Completed(TaskCompletionOutput::ShuffleProducer { manifest }),
			completed(),
		]);

		let error = scheduler
			.dispatch_loop(&stages, &mut dispatcher)
			.await
			.expect_err("exchange id mismatch");

		assert!(matches!(
			error,
			SchedulerError::PartitionManifestMismatch { ref message }
				if message.contains("ExchangeId")
		));
		assert_eq!(dispatcher.seen().len(), 1);
	}

	#[tokio::test]
	async fn consumer_stage_reports_no_progress_until_barrier_opens() {
		let stage = StagePlan {
			stage_id: StageId::new(1),
			upstream_stages: vec![StageId::new(0)],
			tasks: vec![task(StageId::new(1), TaskId::new(0))],
		};
		let mut scheduler = DistributedScheduler::new(registry());
		let mut dispatcher = ScriptedDispatcher::new([completed()]);

		let err = scheduler
			.dispatch_loop(&[stage], &mut dispatcher)
			.await
			.expect_err("barrier remains closed");

		assert_eq!(
			err,
			SchedulerError::NoProgress {
				blocked_stages: vec![StageId::new(1)],
			}
		);
		assert!(dispatcher.seen().is_empty());
	}
}
