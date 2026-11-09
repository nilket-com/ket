//! The control-log and snapshot format boundary between decision authorities.
use super::*;
use crate::test_support::TestDir as TempDir;
use openraft::storage::RaftLogStorageExt;
use std::io::Write;

fn blank(index: u64) -> Entry<WendelonTypeConfig> {
	Entry {
		log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
		payload: EntryPayload::Blank,
	}
}

fn write_segment(dir: &TempDir, pages: &[AlignedBuf]) -> PathBuf {
	let wal = dir.to_path_buf().join("control.wal");
	let mut file = std::fs::File::create(&wal).unwrap();
	for page in pages {
		file.write_all(page.as_slice()).unwrap();
	}
	file.sync_all().unwrap();
	wal
}

async fn replay(wal: &Path, mode: DecisionMode) -> std::io::Result<WALReplay> {
	replay_wal(&crate::storage::BlockingFileOps, wal, None, mode).await
}

fn store_options(mode: DecisionMode) -> WalOptions {
	WalOptions {
		decision_mode: mode,
		..WalOptions::default()
	}
}

async fn open_store(dir: &TempDir, mode: DecisionMode) -> std::io::Result<WendelonLogStore> {
	WendelonLogStore::open_with_options_and_retention(
		dir.to_path_buf().join("control.wal"),
		dir.to_path_buf().join("vote.wnd"),
		dir.to_path_buf().join("snapshot.wnd"),
		Arc::new(crate::storage::BlockingFileOps),
		store_options(mode),
		None,
		None,
	)
	.await
}

/// Every file's bytes, so a refused open can be shown to have changed nothing.
fn file_bytes(dir: &TempDir) -> BTreeMap<std::ffi::OsString, Vec<u8>> {
	std::fs::read_dir(dir.to_path_buf())
		.unwrap()
		.map(|entry| {
			let entry = entry.unwrap();
			(entry.file_name(), std::fs::read(entry.path()).unwrap())
		})
		.collect()
}

#[tokio::test]
async fn each_authority_reads_only_its_own_log_records() {
	let today = DecisionMode::Today;
	let new = DecisionMode::ParentDecisions;
	let dir = crate::test_support::TestDir::new("decision-mode-log");
	let page = serialize_entry_page_as(&blank(1), new.wal_format()).unwrap();
	let wal = write_segment(&dir, &[page]);
	assert_eq!(replay(&wal, new).await.unwrap().entries.len(), 1);
	let error = replay(&wal, today).await.err().unwrap();
	assert_eq!(error.kind(), ErrorKind::InvalidData);
	assert!(
		error.to_string().contains("newer than this build accepts"),
		"{error}"
	);

	let dir = crate::test_support::TestDir::new("decision-mode-log");
	let page = serialize_entry_page_as(&blank(1), today.wal_format()).unwrap();
	let wal = write_segment(&dir, &[page]);
	assert_eq!(replay(&wal, today).await.unwrap().entries.len(), 1);
	assert!(replay(&wal, new).await.is_err());
}

#[test]
fn today_writes_the_bytes_it_always_wrote() {
	let entry = blank(3);
	assert_eq!(
		serialize_entry_page(&entry).unwrap().as_slice(),
		serialize_entry_page_as(&entry, WAL_FORMAT_VERSION)
			.unwrap()
			.as_slice(),
	);
	assert_eq!(
		serialize_checkpoint_page(1, 0, 1, 1).as_slice(),
		serialize_checkpoint_page_as(1, 0, 1, 1, WAL_FORMAT_VERSION).as_slice(),
	);
	assert_eq!(
		snapshot_header_template(&SmState::default()).format_version,
		SNAPSHOT_FORMAT_VERSION,
	);
}

#[test]
fn a_new_mode_snapshot_is_refused_by_today_before_its_body_is_trusted() {
	let new = DecisionMode::ParentDecisions;
	let state = SmState {
		decisions: Some(Default::default()),
		..SmState::default()
	};
	let body = encode_state_body(new, &state).unwrap();
	let header = SnapshotHeader {
		state_sha256: control_artifact_digest_32(&body),
		..snapshot_header_template_as(&state, new.snapshot_format())
	};
	let mut bytes = bytemuck::bytes_of(&header).to_vec();
	bytes.extend_from_slice(&body);
	decode_snapshot_reader_as(new, bytes.as_slice()).unwrap();
	let error = decode_snapshot_reader_as(DecisionMode::Today, bytes.as_slice())
		.err()
		.unwrap();
	assert!(error.contains("newer than this build accepts"), "{error}");
}

#[tokio::test]
async fn a_store_refuses_the_other_authority_and_is_left_unchanged() {
	let today = DecisionMode::Today;
	let new = DecisionMode::ParentDecisions;
	let dir = crate::test_support::TestDir::new("decision-mode-log");
	let page = serialize_entry_page_as(&blank(1), new.wal_format()).unwrap();
	write_segment(&dir, &[page]);
	let before = file_bytes(&dir);
	assert!(open_store(&dir, today).await.is_err());
	assert_eq!(file_bytes(&dir), before);

	let dir = crate::test_support::TestDir::new("decision-mode-log");
	let page = serialize_entry_page_as(&blank(1), today.wal_format()).unwrap();
	write_segment(&dir, &[page]);
	let before = file_bytes(&dir);
	assert!(open_store(&dir, new).await.is_err());
	assert_eq!(file_bytes(&dir), before);
}

#[tokio::test]
async fn a_new_mode_store_starts_with_a_record_today_refuses() {
	let new = DecisionMode::ParentDecisions;
	let dir = crate::test_support::TestDir::new("decision-mode-holder");
	drop(open_store(&dir, new).await.unwrap());
	let wal = dir.to_path_buf().join("control.wal");
	let segments = discover_all_wal_segments(&crate::storage::BlockingFileOps, &wal)
		.await
		.unwrap();
	assert_eq!(segments.len(), 1);
	let bytes = std::fs::read(&segments[0].path).unwrap();
	let header: WALRecordHeader =
		*bytemuck::from_bytes(&bytes[..std::mem::size_of::<WALRecordHeader>()]);
	assert_eq!(header.record_type, REC_CHECKPOINT);
	assert_eq!(header.format_version, new.wal_format());
	// The Today open path, whose gate is the pre-existing one, refuses it and
	// changes nothing, the vote file included.
	let before = file_bytes(&dir);
	let error = open_store(&dir, DecisionMode::Today).await.err().unwrap();
	assert!(
		error.to_string().contains("newer than this build accepts"),
		"{error}"
	);
	assert_eq!(file_bytes(&dir), before);
	// Still a fresh store for its own authority.
	let store = open_store(&dir, new).await.unwrap();
	assert_eq!(
		store.bootstrap_mode().await,
		DurableBootstrapMode::FreshBootstrap
	);
}

#[tokio::test]
async fn the_new_authority_refuses_older_checkpoint_records_alone_or_mixed() {
	let new = DecisionMode::ParentDecisions;
	let today = DecisionMode::Today;
	let only = vec![serialize_checkpoint_page_as(0, 0, 0, 0, today.wal_format())];
	let mixed = vec![
		serialize_entry_page_as(&blank(0), new.wal_format()).unwrap(),
		serialize_checkpoint_page_as(0, 0, 0, 1, today.wal_format()),
	];
	for (tag, pages) in [("checkpoint-only", only), ("mixed", mixed)] {
		let dir = crate::test_support::TestDir::new(&format!("decision-mode-{tag}"));
		write_segment(&dir, &pages);
		let before = file_bytes(&dir);
		let error = open_store(&dir, new).await.err().unwrap();
		assert_eq!(error.kind(), ErrorKind::InvalidData, "{tag}: {error}");
		assert_eq!(file_bytes(&dir), before, "{tag}");
	}
}

#[tokio::test]
async fn a_new_mode_store_writes_its_own_format_and_reopens() {
	let new = DecisionMode::ParentDecisions;
	let dir = crate::test_support::TestDir::new("decision-mode-log");
	let mut store = open_store(&dir, new).await.unwrap();
	store
		.blocking_append(vec![blank(0), blank(1)])
		.await
		.unwrap();
	drop(store);
	let wal = dir.to_path_buf().join("control.wal");
	let segments = discover_all_wal_segments(&crate::storage::BlockingFileOps, &wal)
		.await
		.unwrap();
	let bytes = std::fs::read(&segments[0].path).unwrap();
	let header: WALRecordHeader =
		*bytemuck::from_bytes(&bytes[..std::mem::size_of::<WALRecordHeader>()]);
	assert_eq!(header.format_version, new.wal_format());
	assert_eq!(replay(&wal, new).await.unwrap().entries.len(), 2);
	assert!(open_store(&dir, DecisionMode::Today).await.is_err());
	open_store(&dir, new).await.unwrap();
}

// ============================================================
//  ADR 0699 S2: format 10 carries parent decisions
// ============================================================

use crate::consensus::decisions::tests as fixture;

const PARENT_DECISIONS_V14_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v14.txt");
/// S5a to ADR 0700's format-13 bodies, retained: no batched lease renewals.
/// ADR 0701 only appended a variant, so every format-13 body is unchanged.
const PARENT_DECISIONS_V13_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v13.txt");
/// S4b's format-12 bodies, retained: no generations.
const PARENT_DECISIONS_V12_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v12.txt");
/// S3's format-11 bodies, retained: claims without an operation, no noops.
const PARENT_DECISIONS_V11_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v11.txt");
/// S2's format-10 bodies, retained: no signer registry, stand-in signatures.
const PARENT_DECISIONS_V10_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v10.txt");

fn hex(bytes: &[u8]) -> String {
	bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn manifest() -> BTreeMap<String, String> {
	parse(PARENT_DECISIONS_V14_MANIFEST)
}

/// ADR 0701: the batch pinned in the format-14 corpus, two leases of one holder.
fn golden_lease_batch() -> WendelonRequest {
	let (holder, range) = fixture::universe()[0];
	let next = (range.1 + 1, range.1 + 1_000);
	WendelonRequest::AcquireLeases(
		crate::consensus::LeaseBatch::new(vec![
			fixture::lease(holder, range, 0, 1_000),
			fixture::lease(holder, next, 500, 2_000),
		])
		.unwrap(),
	)
}

fn parse(raw: &str) -> BTreeMap<String, String> {
	raw.lines()
		.filter(|line| !line.trim().is_empty() && !line.starts_with('#'))
		.map(|line| {
			let (name, hex) = line.split_once('=').unwrap();
			(name.to_owned(), hex.to_owned())
		})
		.collect()
}

fn golden_state() -> SmState {
	let mut state = fixture::base(3);
	for (index, request) in fixture::golden_history() {
		apply_request_at_log_index(&mut state, request, index, 0);
	}
	state
}

fn entry(index: u64, request: WendelonRequest) -> Entry<WendelonTypeConfig> {
	Entry {
		log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
		payload: EntryPayload::Normal(request),
	}
}

#[test]
fn the_format_14_bodies_match_their_goldens_and_round_trip() {
	let stored = manifest();
	let state = golden_state();
	let body = encode_state_body(DecisionMode::ParentDecisions, &state).unwrap();
	// The envelope is its version, today's body, then the decision state. Only
	// the decision state is pinned byte for byte: today's body holds hash maps
	// whose encoding order is not stable across instances.
	let decision_bytes = postcard::to_stdvec(state.decisions.as_ref().unwrap()).unwrap();
	let mut expected = vec![crate::consensus::decisions::DECISION_SNAPSHOT_ENVELOPE_VERSION];
	expected.extend(postcard::to_stdvec(&state).unwrap());
	expected.extend(&decision_bytes);
	assert_eq!(body, expected);
	let mut actual = vec![("decisions_golden_history".to_owned(), hex(&decision_bytes))];
	let decisions = fixture::golden_history()
		.into_iter()
		.map(|(_, request)| request)
		.filter(|request| request.is_parent_decisions_only());
	for (n, request) in decisions.enumerate() {
		actual.push((
			format!("decision_{n}"),
			hex(&postcard::to_stdvec(&request).unwrap()),
		));
	}
	actual.push((
		"acquire_leases_0".to_owned(),
		hex(&postcard::to_stdvec(&golden_lease_batch()).unwrap()),
	));
	let moved: Vec<String> = actual
		.iter()
		.filter(|(name, hex)| stored.get(name) != Some(hex))
		.map(|(name, hex)| format!("{name}={hex}"))
		.collect();
	assert!(
		moved.is_empty(),
		"format-14 bodies moved or are missing; expected lines:\n{}",
		moved.join("\n")
	);
	assert_eq!(stored.len(), actual.len());

	let decoded = decode_state_body(DecisionMode::ParentDecisions, &body).unwrap();
	assert_eq!(decoded.decisions, state.decisions);
	assert_eq!(decoded.leases, state.leases);
	assert_eq!(decoded.epoch_history, state.epoch_history);
	let decisions = decoded.decisions.as_ref().unwrap();
	assert_eq!(decisions.retained_bodies(), 2);
	assert_eq!(decisions.retry_keys(), 1);
	assert!(decisions.floor_ms() > 0 && decisions.next_generation() > 2);
	// Today cannot read it as its own body.
	assert!(decode_state_body(DecisionMode::Today, &body).is_err());
	// ADR 0701 appended a variant: every retained format-13 body is unchanged.
	let retained = parse(PARENT_DECISIONS_V13_MANIFEST);
	for (name, hex) in &retained {
		assert_eq!(stored.get(name), Some(hex), "{name}");
	}
	assert_eq!(stored.len(), retained.len() + 1);
}

#[test]
fn each_authority_encodes_only_its_own_state_shape() {
	let today = SmState::default();
	assert_eq!(
		encode_state_body(DecisionMode::Today, &today).unwrap(),
		postcard::to_stdvec(&today).unwrap(),
		"today's body is the bytes it always was"
	);
	let with_decisions = golden_state();
	let error = encode_state_body(DecisionMode::Today, &with_decisions).unwrap_err();
	assert_eq!(error.kind(), ErrorKind::InvalidInput);
	assert!(encode_snapshot(&with_decisions).is_err());
	assert!(encode_state_body(DecisionMode::ParentDecisions, &today).is_err());
}

#[test]
fn a_snapshot_at_any_point_then_replay_equals_the_whole_replay() {
	let history = fixture::golden_history();
	let new = DecisionMode::ParentDecisions;
	let whole = golden_state();
	for cut in 0..=history.len() {
		let mut state = fixture::base(3);
		for (index, request) in &history[..cut] {
			apply_request_at_log_index(&mut state, request.clone(), *index, 0);
		}
		let body = encode_state_body(new, &state).unwrap();
		let mut state = decode_state_body(new, &body).unwrap();
		for (index, request) in &history[cut..] {
			apply_request_at_log_index(&mut state, request.clone(), *index, 0);
		}
		assert_eq!(state.decisions, whole.decisions, "cut {cut}");
		assert_eq!(state.leases, whole.leases, "cut {cut}");
		assert_eq!(state.incarnations, whole.incarnations, "cut {cut}");
		assert_eq!(state.epoch_history, whole.epoch_history, "cut {cut}");
	}
}

#[tokio::test]
async fn a_decision_record_is_written_only_at_format_10_and_replays() {
	let request =
		WendelonRequest::Decision(crate::consensus::decisions::DecisionCommand::AdvanceFloor {
			floor_ms: 1,
			stamp_ms: 2,
		});
	let record = entry(1, request.clone());
	let error = serialize_entry_page_as(&record, WAL_FORMAT_VERSION)
		.err()
		.unwrap();
	assert!(
		error.to_string().contains("cannot carry a parent decision"),
		"{error}"
	);

	let new = DecisionMode::ParentDecisions;
	let dir = crate::test_support::TestDir::new("decision-record");
	let mut store = open_store(&dir, new).await.unwrap();
	store.blocking_append(vec![blank(0), record]).await.unwrap();
	drop(store);
	let wal = dir.to_path_buf().join("control.wal");
	let replayed = replay(&wal, new).await.unwrap();
	assert_eq!(
		replayed.entries.values().last().unwrap().payload,
		EntryPayload::Normal(request)
	);
	assert!(open_store(&dir, DecisionMode::Today).await.is_err());
}

#[tokio::test]
async fn s1_s2_and_s3_development_artifacts_are_refused_by_both_authorities() {
	// S1 wrote format 9; S2 wrote format 10 with no signer registry and
	// stand-in signatures; S3 wrote format 11, whose claims carry no operation.
	// None is converted: each is refused before replay or decode, files
	// unchanged.
	for retired in [9u8, 10, 11, 12] {
		let dir = crate::test_support::TestDir::new(&format!("decision-retired-wal-{retired}"));
		let page = serialize_entry_page_as(&blank(1), retired).unwrap();
		let wal = write_segment(&dir, &[page]);
		for mode in [DecisionMode::Today, DecisionMode::ParentDecisions] {
			let error = replay(&wal, mode).await.err().unwrap();
			assert_eq!(
				error.kind(),
				ErrorKind::InvalidData,
				"{retired} {mode:?}: {error}"
			);
		}
		let before = file_bytes(&dir);
		for mode in [DecisionMode::Today, DecisionMode::ParentDecisions] {
			assert!(open_store(&dir, mode).await.is_err(), "{retired} {mode:?}");
		}
		assert_eq!(file_bytes(&dir), before);

		let state = golden_state();
		let body = encode_state_body(DecisionMode::ParentDecisions, &state).unwrap();
		let header = SnapshotHeader {
			state_sha256: control_artifact_digest_32(&body),
			..snapshot_header_template_as(&state, retired)
		};
		let mut bytes = bytemuck::bytes_of(&header).to_vec();
		bytes.extend_from_slice(&body);
		for mode in [DecisionMode::Today, DecisionMode::ParentDecisions] {
			assert!(
				decode_snapshot_reader_as(mode, bytes.as_slice()).is_err(),
				"{retired} {mode:?}"
			);
		}
	}
	// S2's and S3's retained bodies are not today's: the same history now
	// encodes differently.
	let current = manifest();
	for retained in [
		parse(PARENT_DECISIONS_V10_MANIFEST),
		parse(PARENT_DECISIONS_V11_MANIFEST),
	] {
		for name in [
			"decisions_golden_history",
			"decision_0",
			"decision_1",
			"decision_2",
		] {
			assert_ne!(retained[name], current[name], "{name}");
		}
	}
	// S4b's requests encode as today's; its decision state does not.
	let s4b = parse(PARENT_DECISIONS_V12_MANIFEST);
	assert_ne!(
		s4b["decisions_golden_history"],
		current["decisions_golden_history"]
	);
}

/// Codex S2 hold: the reducer keeps the Raft coordinates readers need. Through
/// the real recovery path, `rsv` is the Reserve entry's index and `dec` the
/// first terminal decision's, across blanks, renewals, duplicates, a competing
/// abort, a floor and a snapshot taken just before the decision.
#[test]
fn recovery_keeps_the_reserve_and_first_decision_log_indices() {
	let new = DecisionMode::ParentDecisions;
	let history = fixture::golden_history();
	let mut entries: Vec<Entry<WendelonTypeConfig>> = history
		.iter()
		.map(|(index, request)| entry(*index, request.clone()))
		.collect();
	let checkpoint = |last: u64| {
		Some(RecoveryCheckpoint {
			commit_index: last,
			e_safe_index: 0,
		})
	};
	// Replay onto the fixture's base (tenant, incarnations, epoch) through
	// `apply_entry_to_state`, the function recovery applies each entry with.
	let replay = |entries: &[Entry<WendelonTypeConfig>]| {
		let mut state = fixture::base(3);
		for entry in entries {
			apply_entry_to_state(&mut state, entry);
		}
		state
	};
	let state = replay(&entries);
	let decisions = state.decisions.as_ref().unwrap();
	let (rsv, body) = decisions
		.bodies()
		.iter()
		.find(|(_, body)| body.status.is_terminal())
		.map(|(rsv, body)| (*rsv, body.clone()))
		.unwrap();
	let reserve_index = history
		.iter()
		.find(|(_, request)| {
			matches!(
				request,
				WendelonRequest::Decision(
					crate::consensus::decisions::DecisionCommand::Reserve { .. }
				)
			)
		})
		.unwrap()
		.0;
	let finalize_index = history
		.iter()
		.find(|(_, request)| {
			matches!(
				request,
				WendelonRequest::Decision(
					crate::consensus::decisions::DecisionCommand::Finalize { .. }
				)
			)
		})
		.unwrap()
		.0;
	assert_eq!(rsv, reserve_index);
	assert_eq!(body.status.dec(), Some(finalize_index));
	assert!(finalize_index > rsv + 1, "a renewal sits between them");

	// Later duplicates and a competing abort, separated by blanks, move nothing.
	let mut next = entries.len() as u64 + 1;
	let claim = body.claim.clone();
	for command in [
		crate::consensus::decisions::DecisionCommand::Finalize {
			key: claim.key,
			request_digest: claim.request_digest,
			acks: fixture::quorum_acks(&claim, rsv),
		},
		fixture::abort_command(&claim, rsv),
	] {
		entries.push(blank(next));
		entries.push(entry(next + 1, WendelonRequest::Decision(command)));
		next += 2;
	}
	let state = replay(&entries);
	assert_eq!(state.decisions.as_ref().unwrap().body(rsv), Some(&body));

	// A snapshot taken just before the decision, then the rest replayed.
	let before: Vec<_> = entries
		.iter()
		.filter(|e| e.log_id.index < finalize_index)
		.cloned()
		.collect();
	let after: Vec<_> = entries
		.iter()
		.filter(|e| e.log_id.index >= finalize_index)
		.cloned()
		.collect();
	let cut_state = replay(&before);
	let cut_body = cut_state
		.decisions
		.as_ref()
		.unwrap()
		.body(rsv)
		.unwrap()
		.clone();
	assert_eq!(
		cut_body.status,
		crate::consensus::decisions::ParentStatus::Reserved
	);
	let bytes = encode_state_body(new, &cut_state).unwrap();
	let mut resumed = decode_state_body(new, &bytes).unwrap();
	for entry in &after {
		apply_entry_to_state(&mut resumed, entry);
	}
	assert_eq!(resumed.decisions, state.decisions);

	// Recovery starts each authority from its own state shape.
	let blanks: Vec<_> = (1..=3).map(blank).collect();
	let inner = recover_sm_inner(new, None, blanks.clone(), checkpoint(3)).unwrap();
	assert_eq!(inner.state.decisions, Some(Default::default()));
	let inner = recover_sm_inner(DecisionMode::Today, None, blanks, checkpoint(3)).unwrap();
	assert!(inner.state.decisions.is_none());
}

/// ADR 0699 S4b: KV versions are owned under the reserved default KV table id,
/// so no schema may take it under ParentDecisions. Today's authority is
/// unchanged.
#[test]
fn a_schema_cannot_take_the_reserved_kv_table_id_under_parent_decisions() {
	use crate::integration::compute::{
		COMPUTE_STORAGE_CONTRACT_VERSION, SchemaGeneration, SchemaIntentId, SchemaIntentOrigin,
		SchemaMutationKind, SchemaObjectKind, SchemaObjectRef, SchemaUpdateEnvelope,
		encode_schema_update_envelope_v1,
	};
	let descriptor = crate::schema::TableSchemaDescriptor::new_with_layout(
		crate::schema::TableObjectId::DEFAULT_KV,
		1,
		"taken",
		"kv",
		&arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
			"id",
			arrow::datatypes::DataType::Int64,
			false,
		)]),
		crate::schema::RowIdentityPolicy::PrimaryKey(crate::schema::PrimaryKeyDescriptor::single(
			"id",
		)),
		crate::schema::LocalityPolicy::IdentityProjection,
		crate::schema::RecordLayoutPolicy::SingleRow,
		crate::schema::SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap();
	let envelope = SchemaUpdateEnvelope {
		contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
		origin: SchemaIntentOrigin::ComputeClient,
		tenant_id: fixture::TENANT,
		object: SchemaObjectRef {
			kind: SchemaObjectKind::Table,
			namespace: "taken".to_owned(),
			name: "kv".to_owned(),
		},
		expected_generation: SchemaGeneration::ZERO,
		requested_generation: SchemaGeneration::new(descriptor.schema_generation),
		intent_id: SchemaIntentId::new([0x4b; 16]),
		mutation: SchemaMutationKind::Create,
		payload: crate::schema::encode_table_schema_descriptor_v1(&descriptor).unwrap(),
	};
	let bytes = encode_schema_update_envelope_v1(&envelope).unwrap();
	let mut state = fixture::base(3);
	let before = state.table_schemas.clone();
	let error = apply_schema_update(&mut state, &bytes).unwrap_err();
	assert!(error.contains("reserved KV table id"), "{error}");
	assert_eq!(state.table_schemas, before);
}

/// ADR 0699 S5a: the survivor and read policy is frozen in mode. A retention
/// change, a table drop and a tenant delete are refused at apply and change
/// nothing; CTAS, a write path outside parent decisions, is refused too.
#[test]
fn table_retention_is_frozen_under_parent_decisions() {
	use crate::consensus::SetTableRetentionPolicy;
	let mut state = fixture::base(3);
	let before = state.table_lifecycle.clone();
	let update = SetTableRetentionPolicy {
		tenant_id: fixture::TENANT,
		table_object_id: fixture::TABLE,
		expected_policy_generation: crate::schema::PolicyGeneration::ZERO,
		new_policy: crate::schema::TableRetentionPolicy::FixedWindow(
			crate::schema::RetentionWindowMillis::try_new(90_000).unwrap(),
		),
		updated_at_hlc: crate::schema::HybridTimestamp {
			physical_ms: 42,
			logical: 1,
			node_id: 1,
		},
	};
	let (response, _) = apply_request_at_log_index(
		&mut state,
		WendelonRequest::SetTableRetentionPolicy(update),
		1,
		0,
	);
	assert!(
		matches!(&response, WendelonResponse::Error(why) if why.contains("frozen")),
		"{response:?}"
	);
	assert_eq!(state.table_lifecycle, before);

	// A table drop is refused too, before any drop state is recorded.
	let drop_id = crate::integration::compute::DropOperationId {
		tenant_id: fixture::TENANT,
		table_object_id: fixture::TABLE,
		schema_generation: crate::integration::compute::SchemaGeneration::new(1),
	};
	let (response, _) = apply_request_at_log_index(
		&mut state,
		WendelonRequest::TableDropPrepare(crate::consensus::TableDropPrepareRequest::current(
			drop_id,
			drop_id.schema_generation,
		)),
		2,
		0,
	);
	assert!(
		matches!(&response, WendelonResponse::Error(why) if why.contains("drops are refused")),
		"{response:?}"
	);
	assert_eq!(state.table_lifecycle, before);

	// A tenant delete is refused and the tenant keeps its state.
	let tenants = state.tenants.clone();
	let (response, _) = apply_request_at_log_index(
		&mut state,
		WendelonRequest::DeleteTenant(fixture::TENANT),
		3,
		0,
	);
	assert!(
		matches!(&response, WendelonResponse::Error(why) if why.contains("tenant deletes are refused")),
		"{response:?}"
	);
	assert_eq!(state.tenants, tenants);

	// CTAS writes outside parent decisions: refused, with no saga recorded.
	let mut operation = [0x54u8; 16];
	operation[6] = 0x70;
	operation[8] = 0x80;
	let (response, _) = apply_request_at_log_index(
		&mut state,
		WendelonRequest::CtasSaga(crate::schema::CtasSagaMutation::DeclareChain {
			tenant_id: fixture::TENANT,
			operation_id: crate::schema::CtasOperationId::new(operation),
			owner_epoch: 1,
			row_count: 1,
			chunk_count: 1,
		}),
		4,
		0,
	);
	assert!(
		matches!(&response, WendelonResponse::Error(why) if why.contains("CTAS is refused")),
		"{response:?}"
	);
}
