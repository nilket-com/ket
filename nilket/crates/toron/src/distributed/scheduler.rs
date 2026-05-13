use std::collections::{BTreeSet, HashMap, HashSet};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;

use crate::distributed::budget::DurationMillis;
use crate::distributed::identity::{AttemptId, QueryId, StageId, TaskIdentity, WorkerId};
use crate::distributed::task::{
	PartitionAddress, PartitionManifest, ProducerPartitionRef, RetryPolicy, TaskCompletionOutput,
	TaskInputBinding, TaskSpec,
};
use crate::distributed::task_state::{
	AcceptanceState, AttemptState, CancelledTaskCount, InvalidatedTask, TaskKey, TaskStateStore,
};
use crate::distributed::worker_registry::{
	PlacementHints, UnixMillis, WorkerEntry, WorkerRegistry,
};
use crate::error::TaskFailureKind;

#[derive(Debug, Clone)]
pub struct StagePlan {
	pub stage_id: StageId,
	pub upstream_stage: Option<StageId>,
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
	async fn dispatch(&mut self, worker: &WorkerEntry, spec: &TaskSpec) -> DispatchOutcome;
}

#[derive(Debug, Clone)]
pub enum DispatchOutcome {
	Completed(TaskCompletionOutput),
	CompletedWithBatches {
		output: TaskCompletionOutput,
		batches: Vec<RecordBatch>,
	},
	Failed(TaskFailureKind),
}

#[derive(Debug)]
pub struct SchedulerRun {
	pub dispatched: Vec<TaskIdentity>,
	pub accepted_manifests: HashMap<TaskKey, PartitionManifest>,
	pub final_results: Vec<RecordBatch>,
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

pub struct DistributedScheduler<Registry> {
	registry: Registry,
	state: TaskStateStore,
	accepted_manifests: HashMap<TaskKey, PartitionManifest>,
	task_dependencies: HashMap<TaskKey, Vec<PartitionAddress>>,
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
		}
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
		self.state.cancel_query(query_id)
	}

	pub async fn dispatch_loop<D>(
		&mut self,
		stages: &[StagePlan],
		dispatcher: &mut D,
	) -> Result<SchedulerRun, SchedulerError>
	where
		D: TaskDispatcher + Send,
	{
		let mut dispatched = Vec::new();
		let mut final_results = Vec::new();
		let mut completed_tasks = BTreeSet::new();
		let expected_task_count = stages.iter().map(|stage| stage.tasks.len()).sum::<usize>();

		while completed_tasks.len() < expected_task_count {
			let mut progressed = false;
			let mut blocked_stages = BTreeSet::new();

			for stage in stages {
				if !self.stage_barrier_open(stage.upstream_stage, stages) {
					blocked_stages.insert(stage.stage_id);
					continue;
				}

				for task in &stage.tasks {
					let key = TaskKey::from_identity(task.spec.identity);
					if completed_tasks.contains(&key) {
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
						| AcceptanceState::Failed { .. }
						| AcceptanceState::Cancelled => continue,
					}

					let worker_id = self
						.registry
						.select_for_attempt(&PlacementHints)
						.ok_or(SchedulerError::NoLiveWorker)?;
					let worker = self
						.registry
						.resolve(worker_id)
						.ok_or(SchedulerError::NoLiveWorker)?;
					let mut attempt_id = self
						.state
						.pending_attempt(key)
						.map(|identity| identity.attempt_id)
						.unwrap_or_else(AttemptId::first);
					let mut attempts_spawned = 0u32;

					loop {
						attempts_spawned += 1;
						let mut spec = self.materialize_task_spec(task.spec.clone())?;
						spec.identity = TaskIdentity::new(
							spec.identity.query_id,
							spec.identity.stage_id,
							spec.identity.task_id,
							attempt_id,
						);
						self.state.spawn_attempt(spec.identity);
						self.state.set_attempt_state(
							spec.identity,
							AttemptState::Dispatched { worker_id },
						);
						self.state
							.set_attempt_state(spec.identity, AttemptState::Running { worker_id });
						dispatched.push(spec.identity);

						match dispatcher.dispatch(&worker, &spec).await {
							DispatchOutcome::Completed(output) => {
								self.accept_dispatch_completion(
									&spec,
									worker_id,
									key,
									output,
									Vec::new(),
									&mut final_results,
								)?;
								self.refresh_drained_producers(stages);
								completed_tasks.insert(key);
								progressed = true;
								break;
							}
							DispatchOutcome::CompletedWithBatches { output, batches } => {
								self.accept_dispatch_completion(
									&spec,
									worker_id,
									key,
									output,
									batches,
									&mut final_results,
								)?;
								self.refresh_drained_producers(stages);
								completed_tasks.insert(key);
								progressed = true;
								break;
							}
							DispatchOutcome::Failed(kind) => {
								let retryable = failure_retryable(&kind);
								if retryable
									&& attempts_spawned < task.retry_policy.max_attempts.get()
								{
									self.state.set_attempt_state(
										spec.identity,
										AttemptState::FailedRetryable { kind },
									);
									attempt_id = attempt_id.next();
									continue;
								}

								self.state.set_attempt_state(
									spec.identity,
									AttemptState::FailedTerminal { kind: kind.clone() },
								);
								self.state.fail_task(key, kind.clone());
								return Err(SchedulerError::TaskFailed { key, kind });
							}
						}
					}
				}
			}

			if !progressed {
				return Err(SchedulerError::NoProgress {
					blocked_stages: blocked_stages.into_iter().collect(),
				});
			}
		}

		Ok(SchedulerRun {
			dispatched,
			accepted_manifests: self.accepted_manifests.clone(),
			final_results,
		})
	}

	fn accept_dispatch_completion(
		&mut self,
		spec: &TaskSpec,
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
			TaskCompletionOutput::ShuffleProducer { manifest } => {
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

	fn stage_barrier_open(&self, upstream_stage: Option<StageId>, stages: &[StagePlan]) -> bool {
		let Some(upstream_stage) = upstream_stage else {
			return true;
		};

		stages
			.iter()
			.find(|stage| stage.stage_id == upstream_stage)
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
	}

	fn materialize_task_spec(&self, mut spec: TaskSpec) -> Result<TaskSpec, SchedulerError> {
		for input in &mut spec.inputs {
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

			let producer_stage_has_manifests = self
				.accepted_manifests
				.values()
				.any(|manifest| manifest.stage_id == *producer_stage_id);
			let matching_manifests = self
				.accepted_manifests
				.values()
				.filter(|manifest| {
					manifest.exchange_id == *exchange_id && manifest.stage_id == *producer_stage_id
				})
				.collect::<Vec<_>>();
			if producer_stage_has_manifests && matching_manifests.is_empty() {
				return Err(SchedulerError::PartitionManifestMismatch {
					message: format!(
						"no accepted manifest for exchange {:?} from producer stage {:?}",
						exchange_id, producer_stage_id
					),
				});
			}

			*producer_partitions = matching_manifests
				.into_iter()
				.flat_map(|manifest| {
					manifest
						.partitions
						.iter()
						.filter(|entry| entry.partition_id == *partition_id)
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
				})
				.collect();
		}

		Ok(spec)
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
			.filter(|stage| stage.upstream_stage == Some(key.stage_id))
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
			.filter(|child| child.upstream_stage == Some(stage_id))
			.all(|child| self.stage_transitively_accepted(child.stage_id, stages))
	}
}

fn task_dependencies(spec: &TaskSpec) -> Vec<PartitionAddress> {
	spec.inputs
		.iter()
		.flat_map(|input| match input {
			TaskInputBinding::Scan => Vec::new(),
			TaskInputBinding::ShuffleConsumer {
				producer_partitions,
				..
			} => producer_partitions
				.iter()
				.map(|partition| partition.address)
				.collect(),
		})
		.collect()
}

fn failure_retryable(kind: &TaskFailureKind) -> bool {
	match kind {
		TaskFailureKind::TaskFailed { retryable, .. } => *retryable,
		TaskFailureKind::BudgetExhausted { dimension, .. } => dimension.retryable(),
		_ => false,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::distributed::budget::{ByteCount, QueryBudget, TaskBudget};
	use crate::distributed::identity::{ExchangeId, PartitionId, QueryId, TaskId, WorkerId};
	use crate::distributed::task::{
		ArrowIpcDigest, PartitionBackend, PartitionEntry, RowCount, TaskInputBinding,
		TaskOutputBinding,
	};
	use crate::distributed::worker_registry::{
		ActiveTaskCount, SimdLaneCount, StaticWorkerRegistry, UnixMillis, WorkerCapabilities,
		WorkerEndpoint, WorkerLease, WorkerResourceState, WorkerStatus,
	};
	use crate::ir::ExecutionPlan;
	use arrow::datatypes::{DataType, Field, Schema};
	use std::collections::VecDeque;
	use std::sync::Arc;
	use wendelon::schema::{
		LocalityPolicy, PhysicalDescriptorFingerprint, PrimaryKeyDescriptorV1, RowIdentityPolicy,
		SchemaEvolutionPolicyV1, TableObjectId, TableSchemaDescriptor,
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
			plan: ExecutionPlan::Scan {
				legacy_table_id: crate::ir::TableId(1),
				table: TableObjectId::new([1; 16]),
				fingerprint: physical_fingerprint(),
				schema: Arc::new(Schema::empty()),
				projection: None,
				pbc_program: None,
				profile_hint: crate::ir::QueryProfileHint::Scan,
				primary_key_columns: Vec::new(),
			},
			inputs: vec![TaskInputBinding::Scan],
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
			RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
			LocalityPolicy::default_table_spread(),
			SchemaEvolutionPolicyV1::AdditiveOnly,
		)
		.expect("descriptor")
		.physical_descriptor_fingerprint
	}

	struct ScriptedDispatcher {
		outcomes: VecDeque<DispatchOutcome>,
		seen: Vec<TaskSpec>,
	}

	impl ScriptedDispatcher {
		fn new(outcomes: impl IntoIterator<Item = DispatchOutcome>) -> Self {
			Self {
				outcomes: outcomes.into_iter().collect(),
				seen: Vec::new(),
			}
		}
	}

	#[async_trait]
	impl TaskDispatcher for ScriptedDispatcher {
		async fn dispatch(&mut self, _worker: &WorkerEntry, spec: &TaskSpec) -> DispatchOutcome {
			self.seen.push(spec.clone());
			self.outcomes.pop_front().expect("scripted outcome")
		}
	}

	fn completed() -> DispatchOutcome {
		DispatchOutcome::Completed(TaskCompletionOutput::FinalResult {
			output_digest: None,
		})
	}

	#[tokio::test]
	async fn single_stage_query_dispatches_and_accepts() {
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stage: None,
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
	async fn stage_barrier_dispatches_consumer_after_upstream_acceptance() {
		let producer = task(StageId::new(0), TaskId::new(0));
		let consumer = task(StageId::new(1), TaskId::new(0));
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stage: None,
				tasks: vec![producer],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stage: Some(StageId::new(0)),
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
	async fn retryable_failure_retries_once_then_accepts() {
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stage: None,
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
	}

	#[tokio::test]
	async fn max_attempts_failure_marks_acceptance_failed() {
		let task = task(StageId::new(0), TaskId::new(0));
		let key = TaskKey::from_identity(task.spec.identity);
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stage: None,
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
			partition_key: Vec::new(),
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
			backend: PartitionBackend::InMemory,
		};
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stage: None,
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
	async fn cancel_query_removes_manifests_and_closes_tasks() {
		let mut producer = task(StageId::new(0), TaskId::new(0));
		producer.spec.output = TaskOutputBinding::ShuffleProducer {
			exchange_id: ExchangeId::new(9),
			partition_count: crate::distributed::task::PartitionCount::new(1),
			partition_key: Vec::new(),
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
			backend: PartitionBackend::InMemory,
		};
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stage: None,
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
			partition_key: Vec::new(),
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
			backend: PartitionBackend::InMemory,
		};
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stage: None,
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
			partition_key: Vec::new(),
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
			backend: PartitionBackend::InMemory,
		};
		let stage = StagePlan {
			stage_id: StageId::new(0),
			upstream_stage: None,
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
			upstream_stage: None,
			tasks: vec![task(StageId::new(0), TaskId::new(0))],
		};
		let key = TaskKey::from_identity(stage.tasks[0].spec.identity);
		let mut scheduler = DistributedScheduler::new(failover_registry());
		let mut first_dispatch = ScriptedDispatcher::new([completed()]);
		scheduler
			.dispatch_loop(&[stage.clone()], &mut first_dispatch)
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
			partition_key: Vec::new(),
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
			backend: PartitionBackend::InMemory,
		};
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stage: None,
				tasks: vec![producer],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stage: Some(StageId::new(0)),
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

		let TaskInputBinding::ShuffleConsumer {
			producer_partitions,
			..
		} = &dispatcher.seen[1].inputs[0]
		else {
			panic!("consumer input should remain a shuffle consumer");
		};
		assert_eq!(producer_partitions.len(), 1);
		assert_eq!(producer_partitions[0].endpoint, endpoint);
		assert_eq!(producer_partitions[0].address.partition_id, partition_id);
	}

	#[tokio::test]
	async fn producer_is_marked_drained_after_downstream_acceptance() {
		let exchange_id = ExchangeId::new(9);
		let partition_id = PartitionId::new(0);
		let mut producer = task(StageId::new(0), TaskId::new(0));
		producer.spec.output = TaskOutputBinding::ShuffleProducer {
			exchange_id,
			partition_count: crate::distributed::task::PartitionCount::new(1),
			partition_key: Vec::new(),
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
			backend: PartitionBackend::InMemory,
		};
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stage: None,
				tasks: vec![producer],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stage: Some(StageId::new(0)),
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
			partition_key: Vec::new(),
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
			backend: PartitionBackend::InMemory,
		};
		let stages = [
			StagePlan {
				stage_id: StageId::new(0),
				upstream_stage: None,
				tasks: vec![producer],
			},
			StagePlan {
				stage_id: StageId::new(1),
				upstream_stage: Some(StageId::new(0)),
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
		assert_eq!(dispatcher.seen.len(), 1);
	}

	#[tokio::test]
	async fn consumer_stage_reports_no_progress_until_barrier_opens() {
		let stage = StagePlan {
			stage_id: StageId::new(1),
			upstream_stage: Some(StageId::new(0)),
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
		assert!(dispatcher.seen.is_empty());
	}
}
