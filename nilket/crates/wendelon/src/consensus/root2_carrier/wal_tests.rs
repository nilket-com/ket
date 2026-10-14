use super::*;
use crate::audit::InMemoryAuditSink;
use crate::consensus::Root2Carrier1;
use std::sync::atomic::{AtomicUsize, Ordering};
fn unhex(hex: &str) -> Vec<u8> {
	(0..hex.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
		.collect()
}
fn manifest(text: &str) -> Vec<(&str, Vec<u8>)> {
	text.lines()
		.filter(|line| !line.is_empty() && !line.starts_with('#'))
		.map(|line| {
			let (n, h) = line.split_once('=').unwrap();
			(n, unhex(h))
		})
		.collect()
}
fn record(payload: &[u8], version: u8) -> std::io::Result<Entry<WendelonTypeConfig>> {
	// Hash-matched body uses the actual production decoder, not postcard as a stand-in.
	let fixed_size = std::mem::size_of::<RaftLogEntryFixed>();
	let fixed = RaftLogEntryFixed {
		kind: LogEntryKind::APPEND,
		_pad1: [0; 7],
		term: 3,
		index: 11,
		epoch: 0,
		leader: SchemaNodeId {
			id: 1,
			_reserved: 0,
		},
		entry_hlc: HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 1,
		},
		payload_len: u32::try_from(payload.len()).unwrap(),
		_pad2: [0; 4],
		payload_hash: control_artifact_digest_32(payload),
	};
	let mut body = bytemuck::bytes_of(&fixed).to_vec();
	body.extend(payload);
	let header = WALRecordHeader {
		record_type: REC_LOG_ENTRY,
		format_version: version,
		_pad1: [0; 6],
		body_len: body.len() as u64,
		log_index: 11,
		term: 3,
	};
	decode_log_record(Path::new("carrier-test.wnd"), 0, &header, &body, fixed_size)
}
#[test]
fn carrier_wal_versions_zero_through_five_use_exact_frozen_dispatch() {
	let manifests = [
		(
			0,
			include_str!("../../../tests/fixtures/consensus_body/wal_v4_entries.txt"),
		),
		(
			1,
			include_str!("../../../tests/fixtures/consensus_body/wal_v1_entries.txt"),
		),
		(
			2,
			include_str!("../../../tests/fixtures/consensus_body/wal_v2_entries.txt"),
		),
		(
			3,
			include_str!("../../../tests/fixtures/consensus_body/wal_v3_entries.txt"),
		),
		(
			4,
			include_str!("../../../tests/fixtures/consensus_body/wal_v4_entries.txt"),
		),
		(
			5,
			include_str!("../../../tests/fixtures/consensus_body/wal_v5_entries.txt"),
		),
	];
	for (version, text) in manifests {
		for (name, payload) in manifest(text) {
			let entry = record(&payload, version).unwrap();
			let current = postcard::to_stdvec(&entry).unwrap();
			if version == 1 && name.starts_with("ctas_saga_provision") {
				let mut projected = payload.clone();
				projected.push(0);
				assert_eq!(current, projected);
			} else {
				assert_eq!(current, payload, "v{version} {name}");
			}
			let mut trailing = payload.clone();
			trailing.push(0);
			assert!(
				record(&trailing, version)
					.unwrap_err()
					.to_string()
					.contains("format mismatch")
			);
			if version == 0 || version == 4 {
				assert_carrier_v4_fixture_round_trip(&payload);
			}
		}
	}
	let current = manifest(include_str!(
		"../../../tests/fixtures/consensus_body/wal_v5_entries.txt"
	));
	let carrier = &current
		.iter()
		.find(|(name, _)| *name == "root2_command")
		.unwrap()
		.1;
	for version in [0, 4, 6, 255] {
		assert!(
			record(carrier, version)
				.unwrap_err()
				.to_string()
				.contains("format mismatch")
		);
	}
	let mut overbound = vec![3, 1, 11, 1, 26, 1];
	overbound.extend([0xb1, 0x80, 0x10]); //262193, no payload
	assert!(
		record(&overbound, 5)
			.unwrap_err()
			.to_string()
			.contains("format mismatch")
	);
	let wrong_version = vec![3, 1, 11, 1, 26, 2];
	assert!(
		record(&wrong_version, 5)
			.unwrap_err()
			.to_string()
			.contains("format mismatch")
	);
}
#[test]
fn carrier_current_v5_corpus_matches_independent_manifest_and_writer_kind() {
	let stored = manifest(include_str!(
		"../../../tests/fixtures/consensus_body/wal_v5_entries.txt"
	));
	let requests = body_format_corpus::canonical_requests();
	assert_eq!(stored.len(), requests.len() + 2);
	for (name, request) in requests {
		let entry = Entry::<WendelonTypeConfig> {
			log_id: LogId::new(CommittedLeaderId::new(3, 1), 11),
			payload: EntryPayload::Normal(request),
		};
		let bytes = postcard::to_stdvec(&entry).unwrap();
		assert_eq!(&bytes, &stored.iter().find(|(n, _)| *n == name).unwrap().1);
		if name == "root2_command" {
			let page = serialize_entry_page(&entry).unwrap();
			let head = std::mem::size_of::<WALRecordHeader>();
			let fixed_size = std::mem::size_of::<RaftLogEntryFixed>();
			let header = *bytemuck::from_bytes::<WALRecordHeader>(&page.as_slice()[..head]);
			let fixed = *bytemuck::from_bytes::<RaftLogEntryFixed>(
				&page.as_slice()[head..head + fixed_size],
			);
			assert_eq!(header.format_version, 5);
			assert_eq!(fixed.kind, LogEntryKind::APPEND);
			assert_eq!(fixed.epoch, 0);
			let decoded = decode_log_record(
				Path::new("carrier-test.wnd"),
				0,
				&header,
				&page.as_slice()[head..head + header.body_len as usize],
				fixed_size,
			)
			.unwrap();
			assert_eq!(postcard::to_stdvec(&decoded).unwrap(), bytes);
		}
	}
}
struct NoTier(AtomicUsize);
#[async_trait::async_trait]
impl TierObserver for NoTier {
	async fn on_slab_tiered(&self, _: &TieredSlab, _: u64) -> std::io::Result<()> {
		self.0.fetch_add(1, Ordering::SeqCst);
		Ok(())
	}
	async fn on_slab_retired(&self, _: SlabId, _: u64) -> std::io::Result<()> {
		self.0.fetch_add(1, Ordering::SeqCst);
		Ok(())
	}
}
#[tokio::test(flavor = "current_thread")]
async fn carrier_apply_is_held_with_metadata_only_and_no_audit_tier_or_cache_change() {
	let sink = Arc::new(InMemoryAuditSink::new());
	let tracker = Arc::new(AuditMaterializationTracker::new());
	let tier = Arc::new(NoTier(AtomicUsize::new(0)));
	let mut sm = WendelonStateMachine::new()
		.with_tier_observer(tier.clone())
		.with_audit_materializer(
			SchemaNodeId {
				id: 1,
				_reserved: 0,
			},
			sink.clone(),
			tracker.clone(),
		);
	let before = sm.state_snapshot().await;
	let admission = sm.subscribe_admission_snapshot();
	let revocations = sm.subscribe_revocation_snapshot();
	let mut finalizations = sm.subscribe_finalizations();
	let request = WendelonRequest::Root2Command(Root2Carrier1::new(&[7], None).unwrap());
	let mut direct = before.clone();
	let (response, event) = apply_request_at_log_index(&mut direct, request.clone(), 11);
	assert_eq!(response, WendelonResponse::Root2Dormant);
	assert!(event.is_none());
	assert_eq!(
		postcard::to_stdvec(&direct).unwrap(),
		postcard::to_stdvec(&before).unwrap()
	);
	let entry = Entry::<WendelonTypeConfig> {
		log_id: LogId::new(CommittedLeaderId::new(3, 1), 11),
		payload: EntryPayload::Normal(request),
	};
	assert!(
		raft_audit_event(
			SchemaNodeId {
				id: 1,
				_reserved: 0
			},
			&entry,
			&response
		)
		.is_none()
	);
	assert_eq!(
		RaftStateMachine::apply(&mut sm, [entry.clone()])
			.await
			.unwrap(),
		vec![WendelonResponse::Root2Dormant]
	);
	let mut after = sm.state_snapshot().await;
	assert_eq!(after.last_applied_log_id, Some(entry.log_id));
	after.last_applied_log_id = before.last_applied_log_id;
	assert_eq!(
		postcard::to_stdvec(&after).unwrap(),
		postcard::to_stdvec(&before).unwrap()
	);
	assert!(sink.events().is_empty());
	assert_eq!(tracker.in_flight_entries(), 0);
	assert_eq!(tracker.lag_entries(), 0);
	assert_eq!(tier.0.load(Ordering::SeqCst), 0);
	assert!(!admission.has_changed().unwrap());
	assert!(!revocations.has_changed().unwrap());
	assert!(finalizations.try_recv().is_err());
}
