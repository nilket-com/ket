#![allow(unused_imports)]

use super::super::support::*;
use super::super::{
	core_invariants::*, network_tls::*, observability_audit::*, observability_telemetry::*,
	predicate_pushdown::*, ring_placement::*, sql_planner_surface::*, storage_wendelon_boundary::*,
};
use super::{
	cancellation::*, descriptor_sync::*, resource_governance::*, sql_ingress::*,
	tenant_admission::*, worker_lifecycle::*,
};

#[test]
pub(crate) fn scheduler_dispatch_loop_uses_acceptance_barrier() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let dispatch_loop = source
		.split("pub async fn dispatch_loop")
		.nth(1)
		.and_then(|rest| rest.split("fn stage_barrier_open").next())
		.expect("dispatch_loop block");
	let barrier = source
		.split("fn stage_barrier_open")
		.nth(1)
		.and_then(|rest| rest.split("fn failure_retryable").next())
		.expect("stage_barrier_open block");

	assert!(dispatch_loop.contains("self.stage_barrier_open"));
	assert!(barrier.contains("AcceptanceState::LiveAccepted"));
	assert!(!barrier.contains("AttemptState::Completed"));
}

#[test]
pub(crate) fn g16_no_non_pinned_join_algorithms_in_runtime_paths() {
	let manifest_dir = manifest_dir();
	let forbidden = [
		"SortMergeJoin",
		"NestedLoopJoin",
		"CrossJoin",
		"SemiJoin",
		"AntiJoin",
		"OuterJoin",
		"LeftJoin",
		"RightJoin",
		"FullJoin",
		"HashJoin",
		"BroadcastJoin",
	];
	let offenders = source_lines(&manifest_dir.join("src"))
		.into_iter()
		.filter(|(path, _, _)| {
			let relative = path.strip_prefix(&manifest_dir).unwrap_or(path);
			relative == Path::new("src/exec.rs") || relative.starts_with("src/distributed")
		})
		.filter(|(_, _, line)| forbidden.iter().any(|marker| line.contains(marker)))
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"non-pinned join algorithm marker found:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn g17_join_ir_surface_has_single_join_variant_and_two_strategies() {
	let execution_source = std::fs::read_to_string(manifest_dir().join("src/ir/execution.rs"))
		.expect("read execution IR");
	let execution_plan = execution_source
		.split("pub enum ExecutionPlan")
		.nth(1)
		.and_then(|rest| rest.split("impl ExecutionPlan").next())
		.expect("ExecutionPlan block");
	let logical_source =
		std::fs::read_to_string(manifest_dir().join("src/ir.rs")).expect("read logical IR");
	let join_kind = logical_source
		.split("pub enum JoinKind")
		.nth(1)
		.and_then(|rest| rest.split("pub enum SetOpAllKind").next())
		.expect("JoinKind block");
	let join_strategy = logical_source
		.split("pub enum JoinStrategy")
		.nth(1)
		.and_then(|rest| rest.split("pub enum BroadcastBuildSide").next())
		.expect("JoinStrategy block");
	let logical_plan = logical_source
		.split("pub enum LogicalPlan")
		.nth(1)
		.expect("LogicalPlan block");
	let join_kind_variants = join_kind
		.lines()
		.filter_map(|line| {
			let trimmed = line.trim();
			trimmed
				.ends_with(',')
				.then(|| trimmed.trim_end_matches(',').to_string())
		})
		.collect::<Vec<_>>();

	assert_eq!(execution_plan.matches("Join {").count(), 1);
	assert_eq!(logical_plan.matches("Join {").count(), 1);
	assert_eq!(
		join_kind_variants,
		vec![
			"Inner",
			"LeftOuter",
			"RightOuter",
			"FullOuter",
			"LeftSemi",
			"RightSemi",
			"LeftAnti",
			"RightAnti",
			// ADR 239: a new keyless cartesian join *kind* on the existing Join node.
			"Cross",
		]
	);
	assert_eq!(enum_variant_count(join_kind), 9);
	assert_eq!(enum_variant_count(join_strategy), 2);
	assert!(join_strategy.contains("HashPartitioned"));
	assert!(join_strategy.contains("Broadcast"));
}

#[test]
pub(crate) fn g18_scheduler_uses_multi_upstream_stage_barrier() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let barrier = source
		.split("fn stage_barrier_open")
		.nth(1)
		.and_then(|rest| rest.split("fn materialize_task_spec").next())
		.expect("stage_barrier_open block");

	assert!(source.contains("pub upstream_stages: Vec<StageId>"));
	assert!(!source.contains("upstream_stage: Option"));
	assert!(barrier.contains("upstream_stages.iter().all"));
}

#[test]
pub(crate) fn adr119_dispatch_loop_has_in_flight_frontier() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let dispatch_loop = source
		.split("pub async fn dispatch_loop")
		.nth(1)
		.and_then(|rest| rest.split("fn accept_dispatch_outcome").next())
		.expect("dispatch_loop block");

	assert!(dispatch_loop.contains("FuturesUnordered"));
	assert!(dispatch_loop.contains("in_flight.push"));
	assert!(dispatch_loop.contains("in_flight.next().await"));
	assert!(source.contains("max_in_flight_tasks"));
	assert!(dispatch_loop.contains("in_flight_capacity_available"));
}

#[test]
pub(crate) fn adr119_expected_dependencies_are_pre_manifest() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let readiness = source
		.split("fn task_ready_for_dispatch")
		.nth(1)
		.and_then(|rest| rest.split("fn shuffle_inputs_fully_sealed").next())
		.expect("readiness block");

	assert!(source.contains("fn expected_producer_tasks"));
	assert!(readiness.contains("expected_producer_tasks(stages"));
	assert!(readiness.contains("live_ref_for_producer"));
	assert!(readiness.contains("sealed_ref_for_producer"));
	assert!(source.contains("fn task_dependencies(spec: &TaskSpec)"));
}

#[test]
pub(crate) fn adr119_streamable_plan_predicate_is_explicit() {
	let task_source = std::fs::read_to_string(manifest_dir().join("src/distributed/task.rs"))
		.expect("read task source");
	let server_source = std::fs::read_to_string(manifest_dir().join("src/server/mod.rs"))
		.expect("read server source");

	assert!(task_source.contains("pub fn is_streamable_exchange_producer"));
	assert!(task_source.contains("fn streamable_unary_plan"));
	assert!(server_source.contains("is_streamable_exchange_producer(spec)"));
}

#[test]
pub(crate) fn adr119_live_refs_are_explicit_not_fake_sealed_refs() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/task.rs"))
		.expect("read task source");
	let live_ref = source
		.split("pub struct LiveProducerPartitionRef")
		.nth(1)
		.and_then(|rest| rest.split("#[derive(Debug, Clone, Serialize").next())
		.expect("live ref block");

	assert!(source.contains("pub enum ShufflePartitionRef"));
	assert!(source.contains("Live(LiveProducerPartitionRef)"));
	assert!(live_ref.contains("address: PartitionAddress"));
	assert!(live_ref.contains("worker_id: WorkerId"));
	assert!(live_ref.contains("endpoint: WorkerEndpoint"));
	assert!(!live_ref.contains("digest"));
	assert!(!live_ref.contains("size_bytes"));
}

#[test]
pub(crate) fn adr119_partition_store_has_open_append_seal_cancel_lifecycle() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store source");

	for method in [
		"open_live",
		"append_live",
		"seal_live",
		"cancel_live",
		"pull_live",
	] {
		assert!(source.contains(method), "missing {method}");
	}
}

#[test]
pub(crate) fn adr119_live_exchange_path_does_not_call_bound_exchange() {
	let source =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let live_producer = source
		.split("async fn execute_live_shuffle_producer")
		.nth(1)
		.and_then(|rest| rest.split("async fn execute_live_shuffle_consumer").next())
		.expect("live producer block");

	assert!(live_producer.contains("open_live"));
	assert!(
		live_producer.contains("append_live")
			|| (live_producer.contains("LiveAppendStreamableBatchSink")
				&& source.contains(".append_live(payload)"))
	);
	assert!(live_producer.contains("drive_streamable_plan"));
	assert!(!live_producer.contains("bound_exchange"));
	assert!(!live_producer.contains("execute_bound"));
}

#[test]
pub(crate) fn adr119_live_shuffle_input_is_stream_backed_not_vec_override() {
	let source =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let live_storage = source
		.split("impl StorageScan for LiveInputStorageScan")
		.nth(1)
		.and_then(|rest| rest.split("struct LiveScanState").next())
		.expect("live storage block");

	assert!(live_storage.contains("pull_live"));
	assert!(live_storage.contains("ScanStream::new"));
	assert!(live_storage.contains("futures::stream::unfold"));
	assert!(!live_storage.contains("ScanOverride"));
}

#[test]
pub(crate) fn adr119_live_consumer_path_does_not_call_execute_bound_on_live_scan() {
	let source =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let live_consumer = source
		.split("async fn execute_live_shuffle_consumer")
		.nth(1)
		.and_then(|rest| rest.split("fn publish_task_output").next())
		.expect("live consumer block");

	assert!(live_consumer.contains("LiveInputStorageScan"));
	assert!(live_consumer.contains("drive_streamable_plan"));
	assert!(!live_consumer.contains("execute_bound"));
}

#[test]
pub(crate) fn adr119_non_streamable_fallback_is_explicit() {
	let source =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let execution = source
		.split("if is_streamable_final_result(spec)")
		.nth(1)
		.and_then(|rest| rest.split("record_worker_telemetry").next())
		.expect("worker execution branch");

	assert!(execution.contains("drive_streamable_plan"));
	assert!(execution.contains("execute_live_shuffle_producer"));
	assert!(execution.contains("execute_live_shuffle_consumer"));
	assert!(execution.contains("executor.execute_bound"));
}

#[test]
pub(crate) fn adr123_task_ready_for_dispatch_relaxes_for_streamable_both() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let readiness = source
		.split("fn task_ready_for_dispatch")
		.nth(1)
		.and_then(|rest| rest.split("fn shuffle_inputs_fully_sealed").next())
		.expect("task_ready_for_dispatch block");
	let materialize = source
		.split("fn materialize_task_spec")
		.nth(1)
		.and_then(|rest| rest.split("fn sealed_ref_for_producer").next())
		.expect("materialize_task_spec block");

	assert!(readiness.contains("is_live_publishing_producer_stage"));
	assert!(readiness.contains("let live_eligible"));
	assert!(readiness.contains("stage.upstream_stages.len() == 1"));
	assert!(readiness.contains("is_streamable_shuffle_consumer(&task.spec)"));
	assert!(readiness.contains("|| streamable_both"));
	assert!(readiness.contains("!contains_broadcast_join(&task.spec.plan)"));
	assert!(readiness.contains("live_input_worker_for_all_shuffle_inputs"));
	assert!(readiness.contains("self.shuffle_inputs_fully_sealed"));
	assert!(materialize.contains("spec_all_shuffle_inputs_from_streamable_producer_stages"));
	assert!(materialize.contains("!contains_broadcast_join(&spec.plan)"));
	assert!(materialize.contains("consumer_streamable || consumer_streamable_both"));
}

#[test]
pub(crate) fn adr123_streamable_unary_plan_still_excludes_join() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/task.rs"))
		.expect("read task source");
	let streamable = source
		.split("fn streamable_unary_plan")
		.nth(1)
		.expect("streamable_unary_plan block");

	assert!(streamable.contains("ExecutionPlan::Join { .. }"));
}

#[test]
pub(crate) fn adr123_drive_streamable_plan_still_errors_on_join() {
	let source =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let drive = source
		.split("fn drive_streamable_plan")
		.nth(1)
		.and_then(|rest| rest.split("fn partition_record_batch").next())
		.expect("drive_streamable_plan block");

	assert!(drive.contains("ExecutionPlan::Join { .. }"));
	assert!(drive.contains("internal: non-streamable plan reached live stream path"));
}

#[test]
pub(crate) fn adr123_multi_upstream_barrier_holds_for_non_streamable_both() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let requires = source
		.split("fn stage_requires_stage_barrier")
		.nth(1)
		.and_then(|rest| rest.split("fn task_ready_for_dispatch").next())
		.expect("stage_requires_stage_barrier block");
	let barrier = source
		.split("fn stage_barrier_open")
		.nth(1)
		.and_then(|rest| rest.split("fn materialize_task_spec").next())
		.expect("stage barrier block");

	assert!(source.contains("self.stage_requires_stage_barrier(stage, stages)"));
	assert!(requires.contains("is_live_publishing_producer_stage"));
	assert!(requires.contains("let streamable_both"));
	assert!(requires.contains("&& !streamable_both"));
	assert!(barrier.contains("upstream_stages.iter().all"));
	assert!(barrier.contains("AcceptanceState::LiveAccepted"));
}

#[test]
pub(crate) fn adr123_worker_bridges_live_refs_to_non_streamable_consumer() {
	let source =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let execution = source
		.split("if is_streamable_final_result(spec)")
		.nth(1)
		.and_then(|rest| rest.split("record_worker_telemetry").next())
		.expect("worker execution branch");
	let live_branch = execution
		.split("} else if self.has_live_shuffle_input(spec) {")
		.nth(1)
		.and_then(|rest| rest.split("} else if overrides.is_empty()").next())
		.expect("live input branch");

	assert!(live_branch.contains("if is_streamable_shuffle_consumer(spec)"));
	assert!(live_branch.contains("self.execute_live_shuffle_consumer"));
	assert!(live_branch.contains("LiveInputStorageScan"));
	assert!(live_branch.contains("live_scan_overrides(spec)"));
	assert!(live_branch.contains("executor.execute_bound(&spec.plan, morsel)"));
}

#[test]
pub(crate) fn adr123_executor_pbc_wendelon_ir_fragmenter_unchanged() {
	for path in [
		"src/exec.rs",
		"src/ir.rs",
		"src/pbc.rs",
		"src/distributed/fragment.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir().join(path)).expect("read source");
		assert!(!source.contains("ADR 123"), "{path}");
		assert!(!source.contains("adr123"), "{path}");
		assert!(!source.contains("ADR 124"), "{path}");
		assert!(!source.contains("adr124"), "{path}");
	}
	for path in ["src/lib.rs", "src/storage/mod.rs"] {
		let source =
			std::fs::read_to_string(wendelon_manifest_dir().join(path)).expect("read wendelon");
		assert!(!source.contains("ADR 123"), "{path}");
		assert!(!source.contains("adr123"), "{path}");
		assert!(!source.contains("ADR 124"), "{path}");
		assert!(!source.contains("adr124"), "{path}");
	}
}

#[test]
pub(crate) fn adr124_live_partition_has_pacing_semaphore_and_farthest_released_index() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store");
	let live_partition = source
		.split("struct LivePartition {")
		.nth(1)
		.and_then(|rest| rest.split("impl LivePartition").next())
		.expect("LivePartition block");

	assert!(live_partition.contains("pacing_semaphore: Arc<Semaphore>"));
	assert!(live_partition.contains("farthest_released_index: usize"));
}

#[test]
pub(crate) fn adr124_live_backlog_depth_is_named_constant() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store");

	assert!(source.contains("const LIVE_BACKLOG_DEPTH: usize = 4;"));
	assert!(source.contains("Semaphore::new(LIVE_BACKLOG_DEPTH)"));
}

#[test]
pub(crate) fn adr124_partition_store_append_live_is_async() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store");

	assert!(source.contains("#[async_trait::async_trait]\npub trait PartitionStore"));
	assert!(source.contains("async fn append_live(&self, payload: PartitionPayload)"));
	for impl_name in [
		"InMemoryPartitionStore",
		"EncryptedSpillPartitionStore",
		"HybridPartitionStore",
	] {
		let marker = format!("#[async_trait::async_trait]\nimpl PartitionStore for {impl_name}");
		assert!(
			source.contains(&marker),
			"missing async_trait impl for {impl_name}"
		);
	}
}

#[test]
pub(crate) fn adr124_append_live_acquires_owned_permit_forget_on_success() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store");
	let in_memory_impl = source
		.split("impl PartitionStore for InMemoryPartitionStore")
		.nth(1)
		.expect("in-memory partition store impl");
	let append_live = in_memory_impl
		.split("async fn append_live")
		.nth(1)
		.and_then(|rest| rest.split("fn seal_live").next())
		.expect("in-memory append_live body");
	let acquire = append_live
		.find("acquire_owned()")
		.expect("append_live acquire");
	let reserve = append_live
		.find("reserve(BudgetDimension::ShuffleBytes")
		.expect("append_live reserve");
	let push = append_live
		.find("live.chunks.push(payload)")
		.expect("append_live push");
	let forget = append_live
		.find("permit.forget()")
		.expect("append_live permit forget");

	assert!(acquire < reserve);
	assert!(reserve < push);
	assert!(push < forget);
}

#[test]
pub(crate) fn adr124_pull_live_releases_only_at_farthest_released_index() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store");
	let guard = "if index == live.farthest_released_index";
	let release = source
		.split(guard)
		.nth(1)
		.and_then(|rest| rest.split("return Some((Ok(chunk)").next())
		.expect("farthest release block");

	assert!(release.contains("live.farthest_released_index += 1"));
	assert!(release.contains("live.pacing_semaphore.add_permits(1)"));
}

#[test]
pub(crate) fn adr124_seal_live_materializes_from_chunks() {
	adr126_seal_live_reassembles_arrow_ipc_not_concat();
}

#[test]
pub(crate) fn adr126_seal_live_reassembles_arrow_ipc_not_concat() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store");
	let in_memory_impl = source
		.split("impl PartitionStore for InMemoryPartitionStore")
		.nth(1)
		.expect("in-memory partition store impl");
	let seal_live = in_memory_impl
		.split("fn seal_live")
		.nth(1)
		.and_then(|rest| rest.split("fn cancel_live").next())
		.expect("seal_live body");

	assert!(seal_live.contains("reassemble_partition_chunks"));
	assert!(!seal_live.contains("bytes.extend_from_slice(&chunk.bytes)"));
	assert!(seal_live.contains("reserve(BudgetDimension::ShuffleBytes"));
	assert!(seal_live.contains("inner.partitions.insert(address, payload)"));
	assert!(seal_live.contains("live.pacing_semaphore.close()"));
}

#[test]
pub(crate) fn adr126_live_append_sink_retains_no_record_batches() {
	let source =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let live_append_sink = source
		.split("struct LiveAppendStreamableBatchSink")
		.nth(1)
		.and_then(|rest| rest.split("struct SyncStreamableBatchSink").next())
		.expect("live append sink block");

	assert!(!live_append_sink.contains("partition_batches"));
	assert!(!live_append_sink.contains("Vec<Vec<RecordBatch>>"));
}

#[test]
pub(crate) fn adr126_live_producer_does_not_publish_task_output() {
	let source =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let live_producer = source
		.split("async fn execute_live_shuffle_producer")
		.nth(1)
		.and_then(|rest| rest.split("async fn execute_live_shuffle_consumer").next())
		.expect("live producer block");

	assert!(!live_producer.contains("publish_task_output"));
	assert!(live_producer.contains("PartitionManifest"));
	assert!(live_producer.contains(".seal_live(manifest)"));
	assert!(live_producer.contains("Vec::new()"));
	assert!(live_producer.contains("TaskCompletionOutput::ShuffleProducer"));
}

#[test]
pub(crate) fn adr126_live_open_carries_schema() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store");
	let live_partition = source
		.split("struct LivePartition {")
		.nth(1)
		.and_then(|rest| rest.split("impl LivePartition").next())
		.expect("LivePartition block");

	assert!(source.contains("use arrow::datatypes::SchemaRef;"));
	assert!(source.contains("schema: SchemaRef"));
	assert!(live_partition.contains("schema: Option<SchemaRef>"));
	assert!(source.contains("live.schema = Some(schema)"));
}

#[test]
pub(crate) fn adr126_seal_live_reserves_sealed_payload() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store");
	let in_memory_impl = source
		.split("impl PartitionStore for InMemoryPartitionStore")
		.nth(1)
		.expect("in-memory partition store impl");
	let seal_live = in_memory_impl
		.split("fn seal_live")
		.nth(1)
		.and_then(|rest| rest.split("fn cancel_live").next())
		.expect("seal_live body");

	let reserve = seal_live
		.find("reserve(BudgetDimension::ShuffleBytes")
		.expect("sealed payload reserve");
	let insert = seal_live
		.find("inner.partitions.insert(address, payload)")
		.expect("sealed payload insert");
	assert!(reserve < insert);
}

#[test]
pub(crate) fn adr124_pull_live_unfolds_by_index_for_replay() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store");
	let in_memory_impl = source
		.split("impl PartitionStore for InMemoryPartitionStore")
		.nth(1)
		.expect("in-memory partition store impl");
	let pull_live = in_memory_impl
		.split("fn pull_live")
		.nth(1)
		.and_then(|rest| rest.split("fn remove_query").next())
		.expect("pull_live body");

	assert!(pull_live.contains("stream::unfold"));
	assert!(pull_live.contains("(inner.clone(), address, 0usize)"));
	assert!(pull_live.contains("live.chunks.get(index)"));
}

#[test]
pub(crate) fn adr127_live_partition_tracks_active_readers() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store");
	let live_partition = source
		.split("struct LivePartition {")
		.nth(1)
		.and_then(|rest| rest.split("impl LivePartition").next())
		.expect("LivePartition block");
	let in_memory_impl = source
		.split("impl PartitionStore for InMemoryPartitionStore")
		.nth(1)
		.expect("in-memory partition store impl");
	let pull_live = in_memory_impl
		.split("fn pull_live")
		.nth(1)
		.and_then(|rest| rest.split("fn remove_query").next())
		.expect("pull_live body");
	let stream_guard = source
		.split("struct LivePartitionReaderGuard")
		.nth(1)
		.and_then(|rest| rest.split("impl Drop for LivePartitionStream").next())
		.expect("LivePartitionReaderGuard block");

	assert!(live_partition.contains("active_readers: usize"));
	assert!(source.contains("active_readers: 0"));
	assert!(pull_live.contains("live.active_readers += 1"));
	assert!(stream_guard.contains("live.active_readers.saturating_sub(1)"));
	assert!(stream_guard.contains("release_live_chunks_if_idle(live)"));
}

#[test]
pub(crate) fn adr127_release_drops_chunks_and_reservations() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store");
	let release = source
		.split("fn release_live_chunks_if_idle")
		.nth(1)
		.and_then(|rest| rest.split("impl InMemoryPartitionStore").next())
		.expect("release helper body");
	let in_memory_impl = source
		.split("impl PartitionStore for InMemoryPartitionStore")
		.nth(1)
		.expect("in-memory partition store impl");
	let seal_live = in_memory_impl
		.split("fn seal_live")
		.nth(1)
		.and_then(|rest| rest.split("fn cancel_live").next())
		.expect("seal_live body");
	let reader_guard = source
		.split("impl LivePartitionReaderGuard")
		.nth(1)
		.and_then(|rest| rest.split("impl Drop for LivePartitionStream").next())
		.expect("reader guard impl");

	assert!(release.contains("live.sealed && live.active_readers == 0"));
	assert!(release.contains("live.chunks.clear()"));
	assert!(release.contains("live.reservations.clear()"));
	assert!(seal_live.contains("inner.partitions.insert(address, payload)"));
	assert!(seal_live.contains("release_live_chunks_if_idle(live)"));
	assert!(reader_guard.contains("release_live_chunks_if_idle(live)"));
}

#[test]
pub(crate) fn adr127_pull_live_falls_back_to_sealed_payload() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store");
	let in_memory_impl = source
		.split("impl PartitionStore for InMemoryPartitionStore")
		.nth(1)
		.expect("in-memory partition store impl");
	let pull_live = in_memory_impl
		.split("fn pull_live")
		.nth(1)
		.and_then(|rest| rest.split("fn remove_query").next())
		.expect("pull_live body");
	let fallback = pull_live
		.split("if live.sealed && live.chunks.is_empty()")
		.nth(1)
		.and_then(|rest| rest.split("if live.sealed").next())
		.expect("sealed fallback branch");

	assert!(pull_live.contains("LivePartitionStream::without_reader_guard"));
	assert!(pull_live.contains(".partitions"));
	assert!(pull_live.contains("PartitionMissingReason::StoreEvicted"));
	assert!(fallback.contains(".partitions"));
	assert!(fallback.contains(".get(&address)"));
	assert!(fallback.contains(".cloned()"));
	assert!(fallback.contains("index == 0"));
	assert!(!fallback.contains("farthest_released_index"));
	assert!(!fallback.contains("add_permits"));
}

#[test]
pub(crate) fn adr124_cancel_live_closes_pacing_semaphore() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/partition_store.rs"))
		.expect("read partition store");
	let in_memory_impl = source
		.split("impl PartitionStore for InMemoryPartitionStore")
		.nth(1)
		.expect("in-memory partition store impl");
	let cancel_live = in_memory_impl
		.split("fn cancel_live")
		.nth(1)
		.and_then(|rest| rest.split("fn pull_live").next())
		.expect("cancel_live body");

	assert!(cancel_live.contains("live.pacing_semaphore.close()"));
}

#[test]
pub(crate) fn adr124_live_shuffle_producer_uses_async_append_sink() {
	let source =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let live_producer = source
		.split("async fn execute_live_shuffle_producer")
		.nth(1)
		.and_then(|rest| rest.split("async fn execute_live_shuffle_consumer").next())
		.expect("live producer block");
	let live_append_sink = source
		.split("impl StreamableBatchSink for LiveAppendStreamableBatchSink")
		.nth(1)
		.and_then(|rest| rest.split("struct SyncStreamableBatchSink").next())
		.expect("live append sink impl");

	assert!(live_producer.contains("LiveAppendStreamableBatchSink"));
	assert!(!live_producer.contains("SyncStreamableBatchSink::new"));
	assert!(live_append_sink.contains(".append_live(payload)"));
	assert!(live_append_sink.contains(".await"));
}

#[test]
pub(crate) fn adr124_no_exec_fragmenter_pbc_wendelon_change() {
	adr123_executor_pbc_wendelon_ir_fragmenter_unchanged();
}

#[test]
pub(crate) fn adr124_streamable_unary_plan_still_rejects_join() {
	adr123_streamable_unary_plan_still_excludes_join();
}

#[test]
pub(crate) fn adr123_streamable_producer_stage_predicate_is_single_task() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let predicate = source
		.split("fn is_live_publishing_producer_stage")
		.nth(1)
		.and_then(|rest| {
			rest.split("pub(crate) fn consumer_supports_live_inputs")
				.next()
		})
		.expect("is_live_publishing_producer_stage block");

	assert!(predicate.contains("stage.tasks.len() == 1"));
	assert!(predicate.contains("is_streamable_exchange_producer(&stage.tasks[0].spec)"));
}

#[test]
pub(crate) fn adr123_v1_covers_all_eight_hashpartitioned_join_kinds() {
	let source =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let gate = source
		.split(
			"async fn multi_upstream_streamable_both_all_hashpartitioned_kinds_execute_correctly",
		)
		.nth(1)
		.and_then(|rest| {
			rest.split("async fn execution_plan_join_still_rejected_by_drive_streamable_plan")
				.next()
		})
		.expect("all-kinds behavior gate");

	for variant in [
		"JoinKind::Inner",
		"JoinKind::LeftOuter",
		"JoinKind::RightOuter",
		"JoinKind::FullOuter",
		"JoinKind::LeftSemi",
		"JoinKind::RightSemi",
		"JoinKind::LeftAnti",
		"JoinKind::RightAnti",
	] {
		assert!(gate.contains(variant), "missing {variant}");
	}
	assert!(source.contains("strategy: JoinStrategy::HashPartitioned"));
}

#[test]
pub(crate) fn adr120_coordinator_has_streaming_cursor_surface() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");

	assert!(coordinator.contains("pub struct DistributedResultStream"));
	assert!(coordinator.contains("pub async fn execute_query_stream"));
	assert!(coordinator.contains("impl futures::Stream for DistributedResultStream"));
	assert!(coordinator.contains("mpsc::Receiver<Result<RecordBatch, QueryError>>"));
}

#[test]
pub(crate) fn adr120_collecting_execute_is_compatibility_wrapper() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let execute = coordinator
		.split("pub async fn execute_query_with_context")
		.nth(1)
		.and_then(|rest| rest.split("fn cancel_query_for_tenant").next())
		.expect("execute_query_with_context block");

	assert!(execute.contains("execute_query_stream_with_context(plan, context)"));
	assert!(execute.contains(".await?"));
	assert!(execute.contains("while let Some(batch) = stream.next().await"));
	assert!(execute.contains("batches.push(batch?)"));
}

#[test]
pub(crate) fn adr120_execute_query_stream_uses_background_driver() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let stream_entry = coordinator
		.split("pub async fn execute_query_stream_with_context")
		.nth(1)
		.and_then(|rest| rest.split("pub async fn execute_query_with_context").next())
		.expect("execute_query_stream_with_context block");
	let driver = coordinator
		.split("async fn run_streaming_read_driver")
		.nth(1)
		.and_then(|rest| rest.split("fn spawn_remote_cancel_fanout_task").next())
		.expect("streaming driver block");

	assert!(stream_entry.contains("mpsc::channel(DISTRIBUTED_RESULT_STREAM_DEPTH)"));
	assert!(stream_entry.contains("tokio::spawn(async move"));
	assert!(!stream_entry.contains("dispatch_loop(&stages"));
	assert!(driver.contains("dispatch_loop(&stages, &dispatcher).await"));
}

#[test]
pub(crate) fn adr120_worker_streams_final_result_taskbatch_frames() {
	let task = std::fs::read_to_string(manifest_dir().join("src/distributed/task.rs"))
		.expect("read task source");
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let worker_sink = server
		.split("struct WorkerTaskBatchSink")
		.nth(1)
		.and_then(|rest| rest.split("fn drive_streamable_plan").next())
		.expect("worker task batch sink block");

	assert!(task.contains("pub fn is_streamable_final_result"));
	assert!(server.contains("is_streamable_final_result(&payload.spec)"));
	assert!(server.contains("final_result_sink.is_some()"));
	assert!(worker_sink.contains("MessageKind::TaskBatch"));
	assert!(worker_sink.contains("send_payload("));
	assert!(server.contains("Vec::new(),"));
}

#[test]
pub(crate) fn adr120_network_dispatcher_forwards_final_taskbatch_immediately() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let client = server
		.split("pub async fn execute_task_on_worker_with_telemetry_and_batch_sender")
		.nth(1)
		.and_then(|rest| {
			rest.split("pub async fn execute_task_on_worker_with_completion")
				.next()
		})
		.expect("remote task client block");

	assert!(server.contains("with_final_result_sender"));
	assert!(client.contains("forward_final_batches"));
	assert!(client.contains("MessageKind::TaskBatch"));
	// ADR 628 B1.C: capacity is reserved first, then the publication gate, then the batch is
	// published at once with the permit (still forwarded immediately, never collected).
	assert!(client.contains("sender.reserve().await"));
	assert!(client.contains("permit.send(Ok(batch))"));
	assert!(client.contains("results.push(batch)"));
}

#[test]
pub(crate) fn adr120_cursor_drop_cancels_active_query_synchronously() {
	let coordinator = std::fs::read_to_string(manifest_dir().join("src/coordinator.rs"))
		.expect("read coordinator");
	let drop_impl = coordinator
		.split("impl Drop for DistributedResultStream")
		.nth(1)
		.and_then(|rest| rest.split("pub struct DistributedQueryCoordinator").next())
		.expect("DistributedResultStream Drop block");

	assert!(drop_impl.contains("active_query"));
	assert!(drop_impl.contains(".cancel(CancellationTrigger::ClientDisconnect)"));
	assert!(drop_impl.contains("CancellationTrigger::ClientDisconnect"));
	assert!(drop_impl.contains("self.cancel_remote"));
}

#[test]
pub(crate) fn adr120_non_streamable_final_result_uses_completion_buffer() {
	let server =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let execution = server
		.split("if is_streamable_final_result(spec)")
		.nth(1)
		.and_then(|rest| rest.split("record_worker_telemetry").next())
		.expect("worker execution branch");

	assert!(execution.contains("final_result_sink.is_some()"));
	assert!(execution.contains("executor.execute_bound"));
	assert!(server.contains(".map(RecordBatch::num_rows)"));
	assert!(server.contains("sum::<usize>() as u64"));
}

#[test]
pub(crate) fn adr120_multi_upstream_barrier_boundary_still_holds() {
	adr123_multi_upstream_barrier_holds_for_non_streamable_both();
}

#[test]
pub(crate) fn adr120_no_sql_rune_ir_change() {
	let manifest_dir = manifest_dir();
	let forbidden = [
		"DistributedResultStream",
		"execute_query_stream",
		"WorkerTaskBatchSink",
		"final_result_sender",
	];
	for path in [
		"src/ir.rs",
		"src/ir/execution.rs",
		"src/pbc.rs",
		"src/planner.rs",
		"src/rune_engine.rs",
		"src/sql_planner.rs",
		"../toron-client/src/protocol.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir.join(path)).expect("read source");
		for marker in forbidden {
			assert!(
				!source.contains(marker),
				"{path} must not contain ADR 120 streaming marker {marker}"
			);
		}
	}
}

#[test]
pub(crate) fn adr120_no_toron_worker_protocol_widening() {
	let wire = std::fs::read_to_string(manifest_dir().join("src/wire.rs")).expect("read wire");
	let message_kind = wire
		.split("pub enum MessageKind")
		.nth(1)
		.and_then(|rest| rest.split("impl MessageKind").next())
		.expect("MessageKind block");

	// ADR 543 adds query lease issue/ack and retirement issue/ack without
	// changing ADR 120's result-stream decision.
	assert_eq!(toron::wire::MessageKind::COUNT, 16);
	assert!(!message_kind.contains("ResultStream"));
	assert!(!message_kind.contains("Cursor"));
}

#[test]
pub(crate) fn adr119_no_wendelon_or_frontend_change() {
	let manifest_dir = manifest_dir();
	let forbidden = [
		"ShufflePartitionRef::Live",
		"LiveProducerPartitionRef",
		"LivePartitionStream",
		"LiveInputStorageScan",
		"is_streamable_exchange_producer",
		"pull_live(",
		"live_partition",
	];
	let toron_frontend_files = [
		manifest_dir.join("src/ir.rs"),
		manifest_dir.join("src/ir/execution.rs"),
		manifest_dir.join("src/pbc.rs"),
		manifest_dir.join("src/planner.rs"),
		manifest_dir.join("src/rune_engine.rs"),
		manifest_dir.join("src/sql_planner.rs"),
		manifest_dir.join("../toron-client/src/protocol.rs"),
	];
	let mut offenders = toron_frontend_files
		.into_iter()
		.flat_map(|path| {
			std::fs::read_to_string(&path)
				.expect("read Toron frontend source")
				.lines()
				.enumerate()
				.map(|(idx, line)| (path.clone(), idx + 1, line.to_string()))
				.collect::<Vec<_>>()
		})
		.collect::<Vec<_>>();
	offenders.extend(source_lines(&wendelon_manifest_dir().join("src")));
	let offenders = offenders
		.into_iter()
		.filter(|(_, _, line)| forbidden.iter().any(|marker| line.contains(marker)))
		.collect::<Vec<_>>();

	assert!(
		offenders.is_empty(),
		"ADR 119 live-shuffle markers escaped Toron distributed/server scope:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}

#[test]
pub(crate) fn adr119_consumer_readiness_uses_running_not_advertisement() {
	let scheduler = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let task_state = std::fs::read_to_string(manifest_dir().join("src/distributed/task_state.rs"))
		.expect("read task state source");

	assert!(scheduler.contains("live_ref_for_producer"));
	assert!(scheduler.contains("preferred_live_consumer_worker"));
	assert!(scheduler.contains("self.state.running_attempt"));
	assert!(task_state.contains("AttemptState::Running"));
	assert!(!scheduler.contains("advertise_partition"));
	assert!(!scheduler.contains("live_advertisement"));
}

#[test]
pub(crate) fn g19_join_hash_spill_uses_m10_async_file_ops() {
	let source = exec_source();
	let spill = source
		.split("struct SpilledJoinBuild")
		.nth(1)
		.and_then(|rest| rest.split("fn encode_join_spill_page").next())
		.expect("SpilledJoinBuild block");

	assert!(source.contains("use wendelon::storage::{AsyncFileOps"));
	assert!(spill.contains("Arc<dyn AsyncFileOps>"));
	assert!(spill.contains(".open("));
	assert!(spill.contains(".write_plain_at("));
	assert!(spill.contains(".read_plain_at("));
	assert!(spill.contains(".fdatasync("));
	assert!(spill.contains(".unlink("));
	for forbidden in [
		"std::fs::remove_file",
		"OpenOptions::new",
		".seek(",
		".read_exact(",
		".write_all(",
	] {
		assert!(
			!spill.contains(forbidden),
			"join hash spill bypasses AsyncFileOps with {forbidden}"
		);
	}
}

#[test]
pub(crate) fn broadcast_join_fragmenter_input_shape_is_architecture_guarded() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let broadcast = source
		.split("fn fragment_broadcast_join_plan")
		.nth(1)
		.and_then(|rest| rest.split("fn producer_task").next())
		.expect("broadcast fragmenter block");

	assert_eq!(broadcast.matches("TaskInputBinding::Scan").count(), 1);
	assert!(broadcast.contains("share: Some(ScanShare"));
	assert!(broadcast.contains("collect_broadcast_star_plan(plan, allocator)"));
	assert!(broadcast.contains("let build_inputs = builds"));
	assert!(broadcast.contains("Vec::with_capacity(1 + build_inputs.len())"));
	assert!(broadcast.contains("inputs.extend(build_inputs.clone())"));
	assert_eq!(
		broadcast
			.matches("TaskInputBinding::ShuffleConsumer")
			.count(),
		1
	);
	assert!(broadcast.contains("partition_id: PartitionId::new(0)"));
	assert!(broadcast.contains("upstream_stages.push(build.stage_id)"));
	assert!(broadcast.contains("rewrite_broadcast_star_probe"));
	assert!(broadcast.contains("FragmentError::BroadcastProbeNotStarLinear"));
	assert!(broadcast.contains("first_scan_metadata(&build_boundary.input)"));
	assert!(source.contains("broadcast_composite_build_fragments_to_single_build_producer"));
	assert!(!broadcast.contains("contains_join(&build_boundary.input)"));
	assert!(!broadcast.contains("share: None"));
}

#[test]
pub(crate) fn adr247_seed_fragmenter_recurses_on_stacked_exchanges() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");
	let base = source
		.split("fn fragment_base_plan")
		.nth(1)
		.and_then(|rest| rest.split("fn fragment_union_plan").next())
		.expect("fragment_base_plan block");

	// ADR 247: a linear stack (exchange_count > 1, non-join/union) recurses through the shared peel
	// instead of erroring.
	assert!(base.contains("if exchange_count > 1"));
	assert!(base.contains(
		"return fragment_leading_exchange_plan(plan, boundary, terminal_output, allocator, config)"
	));
	assert!(
		!base.contains("MultipleExchangeBoundaries"),
		"stacked plans no longer error in fragment_base_plan"
	);
}

#[test]
pub(crate) fn adr247_one_shared_leading_exchange_peel() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");

	// One shared peel routine, reached by both the post-join leading-exchange dispatch and the
	// stacked-exchange path; the old join-specific name is gone.
	assert!(source.contains("fn fragment_leading_exchange_plan"));
	assert!(!source.contains("fragment_post_join_exchange_plan"));
	assert_eq!(
		source
			.matches(
				"fragment_leading_exchange_plan(plan, boundary, terminal_output, allocator, config)"
			)
			.count(),
		2,
		"called from exactly the join dispatch and the stacked-exchange path"
	);

	let peel = source
		.split("fn fragment_leading_exchange_plan")
		.nth(1)
		.and_then(|rest| rest.split("fn fragment_base_plan").next())
		.expect("fragment_leading_exchange_plan block");
	// The shared routine peels the outermost exchange and recurses on its input as a producer.
	assert!(peel.contains("replace_exchange_with_scan"));
	assert!(peel.contains("FragmentTerminalOutput::ShuffleProducer"));
	assert!(peel.contains("shuffle_input_scan(scan_metadata, boundary.input.schema()"));
}

#[test]
pub(crate) fn adr247_multiple_exchange_error_retained_for_unhandled_shapes() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/fragment.rs"))
		.expect("read fragmenter source");

	// The error variant still exists and is still raised — a union with an exchanged input is a shape
	// the dedicated path does not fragment.
	assert!(source.contains("MultipleExchangeBoundaries"));
	let union = source
		.split("fn fragment_union_plan")
		.nth(1)
		.and_then(|rest| rest.split("fn union_producer_tasks").next())
		.expect("fragment_union_plan block");
	assert!(union.contains("FragmentError::MultipleExchangeBoundaries"));
}

#[test]
pub(crate) fn adr247_no_new_ir_or_frontend_surface() {
	let manifest_dir = manifest_dir();
	// Fragmenter-only: the stacked-exchange routine does not leak into IR, binder, lowering, or Rune.
	for path in [
		"src/ir.rs",
		"src/ir/execution.rs",
		"src/binder.rs",
		"src/sql_planner.rs",
		"src/rune_engine.rs",
	] {
		let source = std::fs::read_to_string(manifest_dir.join(path)).expect("read source");
		assert!(
			!source.contains("fragment_leading_exchange_plan"),
			"fragmenter symbol leaked into {path}"
		);
	}
	let offenders = source_lines(&wendelon_manifest_dir().join("src"))
		.into_iter()
		.filter(|(_, _, line)| {
			line.contains("fragment_leading_exchange_plan")
				|| line.contains("MultipleExchangeBoundaries")
		})
		.collect::<Vec<_>>();
	assert!(
		offenders.is_empty(),
		"ADR 247 fragmenter work must not cross into Wendelon:\n{}",
		format_offenders(&manifest_dir, &offenders)
	);
}
