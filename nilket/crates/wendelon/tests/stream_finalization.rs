#[path = "support/control_ring.rs"]
mod support;

use arrow::array::LargeBinaryBuilder;
use arrow::record_batch::RecordBatch;
use openraft::EntryPayload;
use openraft::storage::RaftLogReader;
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;
use wendelon::consensus::{TenantFinalization, WendelonRequest};
use wendelon::network::core::{HRFT_MAGIC, read_frame, write_frame};
use wendelon::network::data_server::STREAM_FINALIZATION_BUFFER;
use wendelon::network::server::HrftService;
use wendelon::network::traits::DataRing;
use wendelon::schema::{
	ClientWriteId, DurabilityLevel, FinalizationMarker, FrameHeader, HybridTimestamp, Identity,
	MsgType, NodeId, StreamFinalizationRequestMeta, TenantId, WriteRequestMeta, WriteResponseMeta,
	WriteStatus, write_payload_schema,
};

// A correctness hang detector for stream progress, not a latency budget.
const FINALIZATION_STREAM_HANG_BOUND: Duration = Duration::from_secs(60);

fn tenant() -> TenantId {
	TenantId::ZERO
}

fn hlc(physical_ms: u64, logical: u32, node_id: u32) -> HybridTimestamp {
	HybridTimestamp {
		physical_ms,
		logical,
		node_id,
	}
}

fn write_meta(epoch: u64, key: &[u8], value: &[u8], sequence_no: u64) -> WriteRequestMeta {
	WriteRequestMeta {
		write_id: ClientWriteId {
			client_uuid: [5u8; 16],
			sequence_no,
			idempotency_ttl_ms: 60_000,
			_pad: 0,
		},
		client_hlc: hlc(10 + sequence_no, 0, 1),
		expected_epoch: epoch,
		tenant_id: tenant(),
		proxy_hop: 0,
		durability: DurabilityLevel::QUORUM,
		await_finalization: 0,
		_pad1: [0; 5],
		proxy_source: NodeId {
			id: 0,
			_reserved: 0,
		},
		key_identity: Identity::blake3_tenant(tenant(), key),
		key_len: key.len() as u32,
		value_len: value.len() as u32,
	}
}

fn kv_batch(key: &[u8], value: &[u8]) -> RecordBatch {
	let schema = Arc::new(write_payload_schema());
	let mut keys = LargeBinaryBuilder::new();
	keys.append_value(key);
	let mut values = LargeBinaryBuilder::new();
	values.append_value(value);
	RecordBatch::try_new(
		schema,
		vec![Arc::new(keys.finish()), Arc::new(values.finish())],
	)
	.unwrap()
}

async fn refresh_leases(node: &wendelon::node::WendelonNode) {
	support::wait_for_local_leader(node).await;
	node.data_server
		.refresh_leases_once(&node.ring, &*node.clock, wendelon::node::LEASE_TTL_MS)
		.await;
	node.handles.lease_manager.abort();
}

async fn write_once(
	factory: &Arc<wendelon::network::factory::TransportProviderFactory>,
	pool: &Arc<wendelon::network::pool::BufPool>,
	target: NodeId,
	sequence_no: u64,
) -> WriteResponseMeta {
	let key = format!("stream-fin-key-{sequence_no}");
	let value = format!("stream-fin-value-{sequence_no}");
	let arrow = support::encode_batch(&kv_batch(key.as_bytes(), value.as_bytes())).unwrap();
	let resp = support::call_frame(
		factory,
		pool,
		target,
		MsgType::WRITE_REQ,
		bytemuck::bytes_of(&write_meta(
			1,
			key.as_bytes(),
			value.as_bytes(),
			sequence_no,
		)),
		Some(&arrow),
		None,
	)
	.await
	.unwrap();
	let meta: WriteResponseMeta =
		*bytemuck::from_bytes(&resp.meta[..std::mem::size_of::<WriteResponseMeta>()]);
	assert_eq!(meta.status, WriteStatus::OK);
	meta
}

async fn open_finalization_stream(
	factory: &Arc<wendelon::network::factory::TransportProviderFactory>,
	target: NodeId,
	start_log_index: u64,
	epoch: u64,
) -> std::io::Result<Box<dyn wendelon::network::transport::TransportStream>> {
	let conn = factory.get_connection(target).await?;
	let mut stream = conn.open_bi().await?;
	let request = StreamFinalizationRequestMeta {
		start_log_index,
		epoch,
	};
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: 1,
		msg_type: MsgType::STREAM_FIN_REQ.0,
		flags: 0,
		stream_id: 0,
		meta_len: std::mem::size_of::<StreamFinalizationRequestMeta>() as u32,
		arrow_len: 0,
		trailing_len: 0,
	};
	write_frame(
		&mut *stream,
		&header,
		bytemuck::bytes_of(&request),
		None,
		None,
	)
	.await?;
	stream.finish().await?;
	Ok(stream)
}

async fn read_marker(
	stream: &mut dyn wendelon::network::transport::TransportStream,
	pool: &Arc<wendelon::network::pool::BufPool>,
) -> std::io::Result<FinalizationMarker> {
	let frame = read_frame(stream, pool).await?;
	if frame.header.msg_type != MsgType::STREAM_FIN_EVT.0 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!(
				"expected STREAM_FIN_EVT, got msg_type=0x{:02x}",
				frame.header.msg_type
			),
		));
	}
	if frame.meta.len() < std::mem::size_of::<FinalizationMarker>() {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"STREAM_FIN_EVT meta too small",
		));
	}
	Ok(*bytemuck::from_bytes(
		&frame.meta[..std::mem::size_of::<FinalizationMarker>()],
	))
}

/// Submit the ADR 079 heartbeat shape through the real public Raft writer.
/// The integration fixture spells out the protocol identity/hash because the
/// production constructor is crate-private; no heartbeat task is disabled.
async fn commit_heartbeat(
	node: &wendelon::node::WendelonNode,
	locality: u64,
) -> FinalizationMarker {
	let epoch = 1u64;
	let commit_hlc = node.hlc.now();
	let mut identity = [0; 32];
	identity[..8].copy_from_slice(&locality.to_le_bytes());
	let domain = b"cdc-heartbeat-locality";
	identity[8..8 + domain.len()].copy_from_slice(domain);
	let mut hash = blake3::Hasher::new();
	hash.update(b"wendelon.cdc.locality-heartbeat.v1");
	hash.update(&locality.to_le_bytes());
	hash.update(&epoch.to_le_bytes());
	hash.update(&commit_hlc.physical_ms.to_le_bytes());
	hash.update(&commit_hlc.logical.to_le_bytes());
	hash.update(&commit_hlc.node_id.to_le_bytes());
	let mut marker = FinalizationMarker {
		log_index: 0,
		term: 0,
		epoch,
		commit_hlc,
		payload_hash: *hash.finalize().as_bytes(),
		e_safe_index: 0,
		durability: DurabilityLevel::QUORUM,
		_pad: [0; 7],
	};
	let response = node
		.raft
		.client_write(WendelonRequest::Finalize(TenantFinalization {
			marker,
			tenant_id: tenant(),
			key_identity: Identity::new(identity),
			charged_bytes: 0,
			record_count: 0,
		}))
		.await
		.unwrap();
	marker.log_index = response.log_id.index;
	marker.term = response.log_id.leader_id.term;
	marker.e_safe_index = response.log_id.index;
	marker
}

#[tokio::test]
async fn stream_finalization_replays_after_floor_and_stays_ordered_under_write_load() {
	let (_node_dir, node) = support::bootstrap_node("stream-finalization-order", 1).await;
	refresh_leases(&node).await;

	let service: Arc<dyn HrftService> = node.router.clone();
	let (factory, pool) = support::loopback_factory(node.local_node, service);

	let seed_a = write_once(&factory, &pool, node.local_node, 1).await;
	let seed_b = write_once(&factory, &pool, node.local_node, 2).await;
	let replay_heartbeat = commit_heartbeat(&node, 17).await;
	let seed_c = write_once(&factory, &pool, node.local_node, 3).await;

	let mut stream = open_finalization_stream(&factory, node.local_node, seed_a.log_index, 1)
		.await
		.unwrap();
	// Receiving a replay marker proves the server has subscribed before the
	// explicit live heartbeat; no sleep is used to guess at that boundary.
	let first = tokio::time::timeout(
		FINALIZATION_STREAM_HANG_BOUND,
		read_marker(&mut *stream, &pool),
	)
	.await
	.expect("the stream must deliver its initial replay marker")
	.unwrap();
	let live_heartbeat = commit_heartbeat(&node, 18).await;

	let live_writes = 48u64;
	let mut write_acks: Vec<WriteResponseMeta> = vec![seed_b, seed_c];
	for seq in 4..(4 + live_writes) {
		write_acks.push(write_once(&factory, &pool, node.local_node, seq).await);
	}

	// Freeze a committed interval: later background heartbeats may continue,
	// but cannot change this prefix ending at the last acknowledged write.
	let end_index = write_acks.last().unwrap().log_index;
	assert!(node.state_machine.last_applied_log_index().await.unwrap() >= end_index);
	// Derive the oracle from raw committed entries, independently of the
	// canonical helper that the stream's replay path also uses.
	let mut reader = node.log_store.clone();
	let entries = reader
		.try_get_log_entries((seed_a.log_index + 1)..=end_index)
		.await
		.unwrap();
	assert_eq!(
		entries.len() as u64,
		end_index - seed_a.log_index,
		"the entire committed interval must still be retained"
	);
	let expected: Vec<FinalizationMarker> = entries
		.into_iter()
		.filter_map(|entry| {
			let EntryPayload::Normal(WendelonRequest::Finalize(finalization)) = entry.payload
			else {
				return None;
			};
			let mut marker = finalization.marker;
			if marker.epoch != 1 {
				return None;
			}
			marker.log_index = entry.log_id.index;
			marker.term = entry.log_id.leader_id.term;
			marker.e_safe_index = entry.log_id.index;
			Some(marker)
		})
		.collect();
	assert_eq!(
		node.log_store
			.finalization_markers_after(seed_a.log_index, end_index, 1)
			.await
			.unwrap(),
		expected,
		"canonical replay reader must agree with independent committed-entry oracle"
	);
	assert!(!expected.is_empty());
	assert!(first.log_index > seed_a.log_index && first.log_index <= end_index);
	let markers = tokio::time::timeout(FINALIZATION_STREAM_HANG_BOUND, async {
		let mut markers = vec![first];
		// Bound both memory and work by the independently committed interval.
		// One deadline covers the whole collection, rather than resetting for
		// every marker a broken stream might keep sending.
		for _ in 1..expected.len() {
			let marker = read_marker(&mut *stream, &pool).await.unwrap();
			assert!(
				marker.log_index > markers.last().unwrap().log_index,
				"each stream receipt must advance its log index"
			);
			assert!(
				marker.log_index <= end_index,
				"stream skipped a finalization in the fixed interval"
			);
			markers.push(marker);
		}
		markers
	})
	.await
	.expect("the stream must deliver the complete fixed finalization interval");
	assert_eq!(
		markers.last().unwrap().log_index,
		end_index,
		"stream must reach the final write without skipping it"
	);
	assert!(markers.windows(2).all(|w| w[0].log_index < w[1].log_index));
	assert!(
		markers
			.iter()
			.all(|m| m.epoch == 1 && m.log_index > seed_a.log_index && m.log_index <= end_index)
	);
	assert_eq!(
		markers, expected,
		"every canonical finalization in the fixed interval must appear once with its complete receipt"
	);

	for heartbeat in [replay_heartbeat, live_heartbeat] {
		assert_eq!(
			markers
				.iter()
				.filter(|marker| **marker == heartbeat)
				.count(),
			1,
			"explicit heartbeat must appear exactly once"
		);
	}
	for ack in &write_acks {
		let matches: Vec<_> = markers
			.iter()
			.filter(|marker| marker.log_index == ack.log_index)
			.collect();
		assert_eq!(
			matches.len(),
			1,
			"every acknowledged write must appear exactly once"
		);
		let marker = matches[0];
		assert_eq!(marker.term, ack.term);
		assert_eq!(marker.epoch, ack.epoch);
		assert_eq!(marker.commit_hlc, ack.commit_hlc);
		assert_eq!(marker.durability, DurabilityLevel::QUORUM);
	}
	let observed_indexes: Vec<_> = markers.iter().map(|marker| marker.log_index).collect();
	let write_indexes: Vec<_> = write_acks.iter().map(|ack| ack.log_index).collect();
	assert_ne!(
		observed_indexes, write_indexes,
		"the old write-only oracle must reject this legitimate mixed heartbeat/write stream"
	);
	assert!(
		seed_b.log_index < replay_heartbeat.log_index
			&& replay_heartbeat.log_index < seed_c.log_index
	);
	assert!(seed_c.log_index < live_heartbeat.log_index && live_heartbeat.log_index < end_index);

	support::abort_node(&node);
}

#[tokio::test]
async fn slow_finalization_consumer_is_dropped_without_stalling_writes() {
	let (_node_dir, node) = support::bootstrap_node("stream-finalization-slow", 1).await;
	refresh_leases(&node).await;

	let mut rx = node.data_server.stream_finalization(0, 1).await.unwrap();
	let write_count = STREAM_FINALIZATION_BUFFER as u64 + 32;

	let writes = tokio::time::timeout(Duration::from_secs(10), async {
		let mut metas = Vec::new();
		for seq in 1..=write_count {
			let key = format!("slow-consumer-key-{seq}");
			let value = format!("slow-consumer-value-{seq}");
			let meta = node
				.data_server
				.write(
					write_meta(1, key.as_bytes(), value.as_bytes(), seq),
					kv_batch(key.as_bytes(), value.as_bytes()),
				)
				.await
				.unwrap()
				.0;
			metas.push(meta);
		}
		metas
	})
	.await
	.expect("writes must not stall behind finalization subscriber backpressure");

	let drained = tokio::time::timeout(Duration::from_secs(2), async {
		let mut markers = Vec::new();
		while let Some(marker) = rx.recv().await {
			markers.push(marker);
		}
		markers
	})
	.await
	.expect("slow consumer should be dropped and the channel should close");

	assert!(!drained.is_empty());
	assert!(drained.len() <= STREAM_FINALIZATION_BUFFER);
	assert!(drained.windows(2).all(|w| w[0].log_index < w[1].log_index));
	assert!(drained.last().unwrap().log_index < writes.last().unwrap().log_index);

	support::abort_node(&node);
}
