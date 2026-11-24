//! Wendelon ADR 0128: a verification call keeps decoded frames while the node's allowance holds
//! them. On a real one-voter ParentDecisions store (rf 1, the local holder), through the real
//! write path:
//! - a dense child's live write reads each of its frames once, and the allowance is back to
//!   zero after it;
//! - four writes in flight share the allowance, at its default and at a size they contend for:
//!   every one commits, each call examines each of its copies, the peak charge stays under
//!   the allowance, and nothing is left charged;
//! - with no allowance, and with one too small for a frame, the same writes commit through the
//!   ordinary read, one per copy, with nothing ever charged.
//!
//! The table is a 16-column approximation of the dev `lineitem`.

use super::one_voter::{one_voter, open, until_settled};
use super::*;
use crate::schema::Identity;
use arrow::array::ArrayRef;
use std::time::{Duration, Instant};

fn lineitem_like() -> TableSchemaDescriptor {
	let mut fields = Vec::new();
	for name in ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber"] {
		fields.push(Field::new(name, DataType::Int64, false));
	}
	for name in ["l_quantity", "l_extendedprice", "l_discount", "l_tax"] {
		fields.push(Field::new(name, DataType::Float64, false));
	}
	for name in [
		"l_returnflag",
		"l_linestatus",
		"l_shipdate",
		"l_commitdate",
		"l_receiptdate",
		"l_shipinstruct",
		"l_shipmode",
		"l_comment",
	] {
		fields.push(Field::new(name, DataType::Utf8, false));
	}
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([0xD2; 16]),
		SchemaGeneration::new(1).get(),
		"tpch",
		"lineitem_like",
		&Schema::new(fields),
		RowIdentityPolicy::AppendOnly,
		crate::schema::LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(crate::schema::MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

/// Rows `from..from + n` of the table.
fn lineitem_rows(descriptor: &TableSchemaDescriptor, from: i64, n: usize) -> RecordBatch {
	let ids: Vec<i64> = (from..from + n as i64).collect();
	let int = |f: fn(i64) -> i64| -> ArrayRef {
		Arc::new(Int64Array::from(
			ids.iter().map(|i| f(*i)).collect::<Vec<_>>(),
		))
	};
	let float = |f: fn(i64) -> f64| -> ArrayRef {
		Arc::new(arrow::array::Float64Array::from(
			ids.iter().map(|i| f(*i)).collect::<Vec<_>>(),
		))
	};
	let text = |f: fn(i64) -> String| -> ArrayRef {
		Arc::new(StringArray::from(
			ids.iter().map(|i| f(*i)).collect::<Vec<_>>(),
		))
	};
	RecordBatch::try_new(
		Arc::new(descriptor.arrow_schema().unwrap()),
		vec![
			int(|i| i),
			int(|i| i * 7 % 200_000),
			int(|i| i * 13 % 10_000),
			int(|i| i % 7 + 1),
			float(|i| (i % 50 + 1) as f64),
			float(|i| (i % 90_000) as f64 + 0.25),
			float(|i| (i % 11) as f64 / 100.0),
			float(|i| (i % 9) as f64 / 100.0),
			text(|i| ["A", "N", "R"][(i % 3) as usize].to_owned()),
			text(|i| ["F", "O"][(i % 2) as usize].to_owned()),
			text(|i| format!("199{}-0{}-1{}", i % 8, i % 9 + 1, i % 9)),
			text(|i| format!("199{}-1{}-0{}", i % 8, i % 3, i % 9 + 1)),
			text(|i| format!("199{}-0{}-2{}", i % 8, i % 9 + 1, i % 9)),
			text(|i| {
				[
					"DELIVER IN PERSON",
					"COLLECT COD",
					"NONE",
					"TAKE BACK RETURN",
				][(i % 4) as usize]
					.to_owned()
			}),
			text(|i| {
				["TRUCK", "MAIL", "SHIP", "AIR", "RAIL", "FOB", "REG AIR"][(i % 7) as usize]
					.to_owned()
			}),
			text(|i| format!("carefully final deposits {i:08}")),
		],
	)
	.unwrap()
}

/// What the verification calls charged to this store's allowance did since the last take: each
/// call's own physical frame reads and copies served from a held frame, in the order the calls
/// ended. Scoped to the store, so no other reader's reads are counted.
fn calls(node: &WendelonNode) -> Vec<(u64, u64)> {
	crate::storage::manager::verifier_calls::take(node.slab.verifier_frame_allowance())
		.into_iter()
		.map(|stats| (stats.reads, stats.reused))
		.filter(|(reads, reused)| reads + reused > 0)
		.collect()
}

fn reads(calls: &[(u64, u64)]) -> u64 {
	calls.iter().map(|(reads, _)| reads).sum()
}

fn copies_examined(calls: &[(u64, u64)]) -> u64 {
	calls.iter().map(|(reads, reused)| reads + reused).sum()
}

/// A traced one-voter store with the table created and its first write made.
async fn store() -> (
	crate::test_support::TestDir,
	WendelonNode,
	TableSchemaDescriptor,
) {
	let (dir, cfg) = one_voter();
	let node = open(&cfg).await;
	until_settled(&node, None).await;
	let descriptor = lineitem_like();
	node.raft
		.client_write(crate::consensus::WendelonRequest::SchemaUpdate(
			schema_update(&descriptor),
		))
		.await
		.unwrap();
	until_settled(&node, Some(descriptor.table_object_id)).await;
	let warm = write_meta(&descriptor, 1, uuid_v7());
	assert_committed(
		&write_retrying(
			&node.data_server,
			warm,
			lineitem_rows(&descriptor, 9_000_000, 1),
		)
		.await,
		"the warm write",
	);
	(dir, node, descriptor)
}

/// The copies and distinct physical frames of the child intents of the write `meta`.
async fn frames_of(
	node: &WendelonNode,
	descriptor: &TableSchemaDescriptor,
	meta: &TableWriteRequestMeta,
) -> (usize, usize) {
	let key = key_of(descriptor, meta);
	let body = node
		.state_machine
		.read_state(move |state| {
			state
				.decisions
				.as_ref()?
				.bodies()
				.values()
				.find(|body| body.claim.key == key)
				.cloned()
		})
		.await
		.expect("the parent's record");
	let index = node.slab.persistent_index().unwrap();
	let (mut copies, mut frames) = (0, std::collections::BTreeSet::new());
	for ordinal in 0..body.claim.children.len() as u16 {
		let intent = index
			.parent_intent(body.rsv, ordinal)
			.await
			.unwrap()
			.expect("the child's intent");
		for version in &intent.manifest {
			for location in node
				.slab
				.version_locations(
					intent.tenant,
					Identity::new(version.key_identity),
					version.hlc,
				)
				.unwrap()
			{
				copies += 1;
				frames.insert((location.slab_id, location.offset, location.length));
			}
		}
	}
	(copies, frames.len())
}

async fn close(node: WendelonNode) {
	tokio::time::timeout(Duration::from_secs(60), node.shutdown())
		.await
		.expect("a complete close")
		.unwrap();
}

/// One live write of `rows` rows from `from`: its metadata, its verification calls and its
/// elapsed time.
async fn live_write(
	node: &WendelonNode,
	descriptor: &TableSchemaDescriptor,
	from: i64,
	rows: usize,
) -> (TableWriteRequestMeta, Vec<(u64, u64)>, Duration) {
	let meta = write_meta(descriptor, rows, uuid_v7());
	crate::storage::manager::verifier_calls::watch(node.slab.verifier_frame_allowance());
	let started = Instant::now();
	let response = node
		.data_server
		.table_write_for_test(meta, lineitem_rows(descriptor, from, rows))
		.await
		.unwrap()
		.0;
	let elapsed = started.elapsed();
	assert_committed(&response, "a live write");
	(meta, calls(node), elapsed)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dense_childs_live_write_reads_each_of_its_frames_once() {
	let (_dir, node, descriptor) = store().await;
	let allowance = node.slab.verifier_frame_allowance().clone();
	let (meta, calls, elapsed) = live_write(&node, &descriptor, 0, 4096).await;
	let (copies, frames) = frames_of(&node, &descriptor, &meta).await;
	eprintln!(
		"ADR0128 dense live write: copies {copies} frames {frames} verifier calls (reads, hits) {calls:?} call_ms {:.1} allowance_peak {} allowance_now {}",
		elapsed.as_secs_f64() * 1000.0,
		allowance.peak(),
		allowance.used()
	);
	assert_eq!((copies, frames), (4096, 4));
	assert_eq!(calls.len(), 1, "one verification call for the one child");
	assert_eq!(
		calls[0],
		(frames as u64, (copies - frames) as u64),
		"one physical read a frame; every other copy from a held frame"
	);
	assert!(allowance.peak() > 0, "the frames were charged");
	assert!(
		allowance.peak() <= crate::storage::manager::VERIFIER_FRAME_ALLOWANCE_BYTES,
		"within the allowance"
	);
	assert_eq!(allowance.used(), 0, "nothing stays charged after the call");
	close(node).await;
}

/// Four writes of 4,096 rows in flight together share the allowance. Every write commits;
/// every call examines each of its copies, by a read or from a held frame; the charge never
/// exceeds the allowance; nothing is left charged. How many copies each call reads depends on
/// how the calls interleave, so the counts are reported and bounded, not fixed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_writes_in_flight_share_the_allowance_and_all_commit() {
	four_in_flight(None).await;
}

/// The same four writes with room for about one write's frames (2 MiB): the calls contend, a
/// frame without room is read and not kept, and every write still commits within the smaller
/// allowance.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_writes_contending_for_a_small_allowance_all_commit() {
	four_in_flight(Some(2 * 1024 * 1024)).await;
}

async fn four_in_flight(limit: Option<u64>) {
	let (_dir, node, descriptor) = store().await;
	let allowance = node.slab.verifier_frame_allowance().clone();
	let limit = match limit {
		Some(limit) => {
			allowance.set_limit_for_test(limit);
			limit
		}
		None => crate::storage::manager::VERIFIER_FRAME_ALLOWANCE_BYTES,
	};
	crate::storage::manager::verifier_calls::watch(&allowance);
	let started = Instant::now();
	let mut tasks = tokio::task::JoinSet::new();
	let mut metas = Vec::new();
	for w in 0..4i64 {
		let meta = write_meta(&descriptor, 4096, uuid_v7());
		metas.push(meta);
		let (server, batch) = (
			node.data_server.clone(),
			lineitem_rows(&descriptor, w * 4096, 4096),
		);
		tasks.spawn(async move {
			let started = Instant::now();
			let response = server.table_write_for_test(meta, batch).await.unwrap().0;
			(response, started.elapsed())
		});
	}
	let mut times = Vec::new();
	while let Some(done) = tasks.join_next().await {
		let (response, elapsed) = done.unwrap();
		assert_committed(&response, "a write in flight");
		times.push(elapsed.as_secs_f64() * 1000.0);
	}
	let calls = calls(&node);
	let (mut copies, mut frames) = (0, 0);
	for meta in &metas {
		let (c, f) = frames_of(&node, &descriptor, meta).await;
		copies += c;
		frames += f;
	}
	times.sort_by(f64::total_cmp);
	eprintln!(
		"ADR0128 four in flight (allowance {limit}): copies {copies} frames {frames} verifier calls (reads, hits) {calls:?} reads {} call_ms {times:.1?} wall_ms {:.1} allowance_peak {} allowance_now {}",
		reads(&calls),
		started.elapsed().as_secs_f64() * 1000.0,
		allowance.peak(),
		allowance.used()
	);
	assert_eq!(copies, 4 * 4096);
	assert_eq!(calls.len(), 4, "one verification call a write");
	for (reads, hits) in &calls {
		assert_eq!(reads + hits, 4096, "each call examines each of its copies");
		assert!(*reads >= 4, "at least a read a frame");
	}
	assert_eq!(copies_examined(&calls), copies as u64);
	assert!(reads(&calls) >= frames as u64);
	assert!(allowance.peak() <= limit, "within the allowance");
	assert_eq!(
		allowance.used(),
		0,
		"nothing stays charged after quiescence"
	);
	close(node).await;
}

/// No allowance, and an allowance smaller than one decoded frame: the same write commits with
/// the ordinary read a copy, and nothing is ever charged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_room_for_a_frame_the_write_commits_through_the_ordinary_read() {
	for limit in [0u64, 4_096] {
		let (_dir, node, descriptor) = store().await;
		let allowance = node.slab.verifier_frame_allowance().clone();
		allowance.set_limit_for_test(limit);
		// The store's first write was verified under the default allowance.
		let peak_before = allowance.peak();
		let (meta, calls, elapsed) = live_write(&node, &descriptor, 0, 4096).await;
		let (copies, frames) = frames_of(&node, &descriptor, &meta).await;
		eprintln!(
			"ADR0128 allowance {limit}: copies {copies} frames {frames} verifier calls (reads, hits) {calls:?} call_ms {:.1} allowance_peak {} (before the write {peak_before})",
			elapsed.as_secs_f64() * 1000.0,
			allowance.peak()
		);
		assert_eq!(
			calls,
			vec![(copies as u64, 0)],
			"a read a copy, as before, and no hit"
		);
		assert_eq!(
			(allowance.peak(), allowance.used()),
			(peak_before, 0),
			"nothing was charged by this write"
		);
		close(node).await;
	}
}
