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
		.and_then(|rest| rest.split("pub enum JoinStrategy").next())
		.expect("JoinKind block");
	let join_strategy = logical_source
		.split("pub enum JoinStrategy")
		.nth(1)
		.and_then(|rest| rest.split("pub enum InsertLiteral").next())
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
		]
	);
	assert_eq!(enum_variant_count(join_kind), 8);
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
	assert!(live_producer.contains("append_live"));
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
pub(crate) fn adr119_multi_upstream_barrier_untouched() {
	let source = std::fs::read_to_string(manifest_dir().join("src/distributed/scheduler.rs"))
		.expect("read scheduler source");
	let barrier = source
		.split("fn stage_barrier_open")
		.nth(1)
		.and_then(|rest| rest.split("fn materialize_task_spec").next())
		.expect("stage barrier block");

	assert!(source.contains("stage_requires_stage_barrier"));
	assert!(barrier.contains("upstream_stages.iter().all"));
	assert!(barrier.contains("AcceptanceState::LiveAccepted"));
}

#[test]
pub(crate) fn adr119_non_streamable_fallback_is_explicit() {
	let source =
		std::fs::read_to_string(manifest_dir().join("src/server/mod.rs")).expect("read server");
	let execution = source
		.split("let (results, output) = if is_streamable_exchange_producer(spec)")
		.nth(1)
		.and_then(|rest| rest.split("record_worker_telemetry").next())
		.expect("worker execution branch");

	assert!(execution.contains("execute_live_shuffle_producer"));
	assert!(execution.contains("execute_live_shuffle_consumer"));
	assert!(execution.contains("executor.execute_bound"));
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
		manifest_dir.join("src/sql_wire.rs"),
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
	let source =
		std::fs::read_to_string(manifest_dir().join("src/exec.rs")).expect("read execution source");
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
	assert_eq!(
		broadcast
			.matches("TaskInputBinding::ShuffleConsumer")
			.count(),
		1
	);
	assert!(broadcast.contains("partition_id: PartitionId::new(0)"));
	assert!(!broadcast.contains("share: None"));
}
