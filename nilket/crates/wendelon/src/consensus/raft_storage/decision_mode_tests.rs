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

/// Wendelon ADR 0119 D6, then ADR 0120 D9 and ADR 0121 D1: a ParentDecisions build
/// refuses a ParentDecisions snapshot of the previous format (33, ADR 0120's) and of a later one before
/// its body is trusted; snapshot install decodes through this same reader
/// (`decode_snapshot_file`), so an install of either is refused before anything
/// is published.
#[test]
fn a_parent_decisions_snapshot_of_another_format_is_refused_before_its_body_is_trusted() {
	let new = DecisionMode::ParentDecisions;
	assert_eq!(new.snapshot_format(), 34);
	let state = SmState {
		decisions: Some(Default::default()),
		..SmState::default()
	};
	let body = encode_state_body(new, &state).unwrap();
	for format in [new.snapshot_format() - 1, new.snapshot_format() + 1] {
		let header = SnapshotHeader {
			state_sha256: control_artifact_digest_32(&body),
			..snapshot_header_template_as(&state, format)
		};
		let mut bytes = bytemuck::bytes_of(&header).to_vec();
		bytes.extend_from_slice(&body);
		assert!(
			decode_snapshot_reader_as(new, bytes.as_slice()).is_err(),
			"format {format}"
		);
	}
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
	// ADR 0104 (wendelon): log state is written only under an identity.
	store.write_identity(IDENTITY).await.unwrap();
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

const PARENT_DECISIONS_V34_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v34.txt");
/// Wendelon ADR 0120's format-33 bodies, retained. Wendelon ADR 0121 moved only
/// the decision state (its retention binding, appended) and added the bind
/// command and the retention cut's shapes.
const PARENT_DECISIONS_V33_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v33.txt");
/// Wendelon ADR 0119's format-32 bodies, retained. Wendelon ADR 0120 moved only
/// the decision state (its CTAS part, appended) and added the CTAS commands.
const PARENT_DECISIONS_V32_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v32.txt");
/// Wendelon ADR 0118's format-31 bodies, retained. Wendelon ADR 0119 moved and
/// added none.
const PARENT_DECISIONS_V31_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v31.txt");
/// Wendelon ADR 0113 to ADR 0117's format-30 bodies, retained. Wendelon ADR 0118
/// moved and added none: its drop index is derived, not serialized.
const PARENT_DECISIONS_V30_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v30.txt");
/// Wendelon ADR 0112's format-29 bodies, retained. Wendelon ADR 0113 moved and added
/// none: its table-operation index is derived, not serialized.
const PARENT_DECISIONS_V29_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v29.txt");
/// Wendelon ADR 0108 to ADR 0111's format-28 bodies, retained. Wendelon ADR 0112
/// moved none (the corpus pinned no delete) and added a delete statement's claim
/// and noop.
const PARENT_DECISIONS_V28_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v28.txt");
/// Wendelon ADR 0106's format-27 bodies, retained. Wendelon ADR 0108 moved three
/// (the decision state, which gained the index receipts; the lease batch, whose
/// grants carry the holder's store; the promoted replacement record, which
/// gained its activation) and added the activation, the index receipt and an
/// activated replacement record.
const PARENT_DECISIONS_V27_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v27.txt");
/// Wendelon ADR 0105's format-26 bodies, retained. Wendelon ADR 0106 moved one
/// (the promoted replacement record, which gained its recovery) and added the
/// recovery statement.
const PARENT_DECISIONS_V26_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v26.txt");
/// Wendelon ADR 0104's format-25 bodies, retained. Wendelon ADR 0105 moved
/// none: it added the promotion request and the promoted replacement record.
const PARENT_DECISIONS_V25_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v25.txt");
/// ADR 0736 to ADR 0103's format-24 bodies, retained. ADR 0104 (wendelon) changed
/// one: the decision state, which gained the Raft instance records; and added
/// the three instance commands.
const PARENT_DECISIONS_V24_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v24.txt");
/// ADR 0729 to ADR 0735's format-23 bodies, retained. ADR 0736 changed one: the
/// decision state, which gained the backup hold; and added the two hold commands.
const PARENT_DECISIONS_V23_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v23.txt");
/// ADR 0725 to ADR 0728's format-18 bodies, retained (formats 20 and 22 moved
/// no body of this corpus). ADR 0729 changed two: the decision state, whose
/// lineages gained fields, and the lease batch, which gained its stamp and a
/// generation per grant. Every other body is unchanged.
const PARENT_DECISIONS_V18_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v18.txt");
/// ADR 0720 to ADR 0724's format-17 bodies, retained. ADR 0725 only appended a
/// variant, so every format-17 body is unchanged; what changed is how a bare
/// slab operation applies.
const PARENT_DECISIONS_V17_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v17.txt");
/// ADR 0704 to ADR 0719's format-16 bodies, retained: a decision state with no
/// sealed membership. ADR 0720 changed no command body, so every format-16 body
/// but the decision state is unchanged.
const PARENT_DECISIONS_V16_MANIFEST: &str =
	include_str!("../../../tests/fixtures/consensus_body/parent_decisions_v16.txt");
/// ADR 0701 to ADR 0703's format-14 bodies, retained: no certified tier provenance.
/// ADR 0704 changed no decision or lease body, so every format-14 body is unchanged.
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
	parse(PARENT_DECISIONS_V34_MANIFEST)
}

/// ADR 0725: the three owner-signed operation kinds pinned in the format-18
/// corpus, each signed by its owner's key (Ed25519 signatures are deterministic).
fn golden_owner_ops() -> [(&'static str, WendelonRequest); 3] {
	use crate::consensus::owner_ops::SignedOwnerOp;
	use crate::consensus::slab_ops::{CancelOp, OpKey};
	let signer = fixture::signer(1);
	let owner = crate::consensus::OwnerRef::new(signer.node, signer.incarnation);
	let key = fixture::test_key(1);
	let (_, WendelonRequest::SlabOp(tier)) = golden_first_publication(owner) else {
		unreachable!("the golden first publication is a slab operation")
	};
	let cancel = CancelOp {
		key: OpKey { owner, op_seq: 7 },
		target_digest: tier.command.digest(),
	};
	[
		(
			"owner_op_submit",
			WendelonRequest::OwnerOp(SignedOwnerOp::submit_for_test(&key, tier)),
		),
		(
			"owner_op_cancel",
			WendelonRequest::OwnerOp(SignedOwnerOp::cancel_for_test(&key, cancel)),
		),
		(
			"owner_op_ack",
			WendelonRequest::OwnerOp(SignedOwnerOp::ack(&key, owner, vec![7, 9])),
		),
	]
}

/// A slab's first, uncertified tier pointer as operation 7 of `owner`.
fn golden_first_publication(owner: crate::consensus::OwnerRef) -> (u64, WendelonRequest) {
	use crate::consensus::slab_ops::{SlabCommand, SlabOp, TierOp};
	let tier = TieredSlab {
		slab: owner.slab(crate::schema::SlabId::new(7)),
		l2_uri: "l2://slab/7/first".to_string(),
		payload_hash: [0x77; 32],
		is_encrypted: false,
		tenant_id: None,
		tenant_kek_id: None,
		stats_format_version: None,
		stats_block_len: None,
		stats_block_hash128: None,
		tombstone_metadata: crate::schema::TombstoneMetadata::default(),
		supersession_metadata: crate::schema::SupersessionMetadata::default(),
		certified: None,
	};
	(
		7,
		WendelonRequest::SlabOp(SlabOp {
			op_seq: 7,
			command: SlabCommand::Tier(TierOp {
				tier,
				replaces: None,
			}),
		}),
	)
}

/// ADR 0704: the certified Tier and Retire pinned in the format-16 corpus, each
/// with provenance naming its own operation and source, as apply requires.
fn golden_certified_ops() -> [(&'static str, WendelonRequest); 2] {
	use crate::consensus::slab_ops::{
		CertifiedProvenance, CertifiedRetirement, OpKey, RetireOp, SlabCommand, SlabOp, TierOp,
	};
	let owner = super::body_format_corpus::canonical_owner();
	let source = super::body_format_corpus::canonical_source_generation();
	let slab = owner.slab(crate::schema::SlabId::new(3));
	let tier = TieredSlab {
		slab,
		l2_uri: "l2://slab/3/certified".to_string(),
		payload_hash: [0x33; 32],
		is_encrypted: false,
		tenant_id: None,
		tenant_kek_id: None,
		stats_format_version: None,
		stats_block_len: None,
		stats_block_hash128: None,
		tombstone_metadata: crate::schema::TombstoneMetadata::default(),
		supersession_metadata: crate::schema::SupersessionMetadata::default(),
		certified: Some(CertifiedProvenance {
			op: OpKey { owner, op_seq: 1 },
			source: source.clone(),
			postimage_rows_digest: [0x55; 32],
		}),
	};
	[
		(
			"certified_tier_op",
			WendelonRequest::SlabOp(SlabOp {
				op_seq: 1,
				command: SlabCommand::Tier(TierOp {
					tier,
					replaces: Some(source.clone()),
				}),
			}),
		),
		(
			"certified_retire_op",
			WendelonRequest::SlabOp(SlabOp {
				op_seq: 2,
				command: SlabCommand::Retire(RetireOp {
					slab,
					expected: source.clone(),
					certified: Some(CertifiedRetirement {
						op: OpKey { owner, op_seq: 2 },
						slab,
						source,
					}),
				}),
			}),
		),
	]
}

/// ADR 0701: the batch pinned in the format-14 corpus, two leases of one holder.
fn golden_lease_batch() -> WendelonRequest {
	let (holder, range) = fixture::universe()[0];
	let next = (range.1 + 1, range.1 + 1_000);
	WendelonRequest::AcquireLeases(
		crate::consensus::LeaseBatch::new(
			1_700_000_000_000,
			vec![
				crate::consensus::LeaseBatchGrant {
					lease: fixture::lease(holder, range, 0, 1_000),
					expected_generation: Some(3),
					holder_incarnation: fixture::inc(holder),
				},
				crate::consensus::LeaseBatchGrant {
					lease: fixture::lease(holder, next, 500, 2_000),
					expected_generation: None,
					holder_incarnation: fixture::inc(holder),
				},
			],
		)
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
fn the_format_34_bodies_match_their_goldens_and_round_trip() {
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
		// ADR 0729: the history's grants are lease batches now. Their body is
		// pinned once, as `acquire_leases_0`; `decision_N` stays the Nth decision.
		.filter(|request| {
			request.is_parent_decisions_only()
				&& !matches!(request, WendelonRequest::AcquireLeases(_))
		});
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
	for (name, request) in golden_certified_ops() {
		actual.push((
			name.to_owned(),
			hex(&postcard::to_stdvec(&request).unwrap()),
		));
	}
	for (name, request) in golden_owner_ops() {
		actual.push((
			name.to_owned(),
			hex(&postcard::to_stdvec(&request).unwrap()),
		));
	}
	// ADR 0736: the two backup hold commands.
	for (name, command) in [
		(
			"begin_backup_hold",
			crate::consensus::decisions::DecisionCommand::BeginBackupHold {
				id: 0x0736_0736_0736_0736_0736_0736_0736_0736,
			},
		),
		(
			"release_backup_hold",
			crate::consensus::decisions::DecisionCommand::ReleaseBackupHold {
				id: 0x0736_0736_0736_0736_0736_0736_0736_0736,
				entered_at: 736,
			},
		),
	] {
		actual.push((
			name.to_owned(),
			hex(&postcard::to_stdvec(&WendelonRequest::Decision(command)).unwrap()),
		));
	}
	// ADR 0104 (wendelon): the three instance commands.
	for (name, command) in [
		(
			"grant_instance",
			crate::consensus::decisions::DecisionCommand::GrantInstance {
				birth: [0x04; 16],
				slot: 104,
				kind: crate::consensus::decisions::instances::GrantKind::Replacement {
					token: 0x0104_0104_0104_0104_0104_0104_0104_0104,
				},
			},
		),
		(
			"begin_replacement",
			crate::consensus::decisions::DecisionCommand::BeginReplacement {
				token: 0x0104_0104_0104_0104_0104_0104_0104_0104,
				slot: 104,
			},
		),
		(
			"complete_replacement",
			crate::consensus::decisions::DecisionCommand::CompleteReplacement {
				token: 0x0104_0104_0104_0104_0104_0104_0104_0104,
			},
		),
	] {
		actual.push((
			name.to_owned(),
			hex(&postcard::to_stdvec(&WendelonRequest::Decision(command)).unwrap()),
		));
	}
	// Wendelon ADR 0105: the promotion request and a promoted replacement.
	let promotion = crate::consensus::PromoteIncarnation {
		node: 105,
		token: 0x0105_0105_0105_0105_0105_0105_0105_0105,
		r_new: crate::consensus::decisions::instances::ALLOCATED_BASE,
		birth: [0x05; 16],
		expected: crate::consensus::StoreIncarnation::from_raw(0x0104),
		incarnation: crate::consensus::StoreIncarnation::from_raw(0x0105),
	};
	actual.push((
		"promote_incarnation".to_owned(),
		hex(&postcard::to_stdvec(&WendelonRequest::PromoteIncarnation(promotion)).unwrap()),
	));
	actual.push((
		"promoted_replacement".to_owned(),
		hex(
			&postcard::to_stdvec(&crate::consensus::decisions::instances::Replacement {
				slot: 105,
				r_old: 105,
				pre_voters: [1, 2, 105].into_iter().collect(),
				phase: crate::consensus::decisions::instances::ReplacementPhase::Done {
					r_new: crate::consensus::decisions::instances::ALLOCATED_BASE,
				},
				promoted: Some(crate::consensus::decisions::instances::Promotion {
					expected: promotion.expected,
					incarnation: promotion.incarnation,
				}),
				recovered: None,
				activated: None,
			})
			.unwrap(),
		),
	));
	// Wendelon ADR 0106: the signed recovery statement.
	let recovery = crate::consensus::decisions::RecoveryBody {
		token: 0x0106_0106_0106_0106_0106_0106_0106_0106,
		slot: 1,
		r_new: crate::consensus::decisions::instances::ALLOCATED_BASE,
		birth: [0x06; 16],
		store: crate::consensus::StoreIncarnation::from_raw(0x0106),
		gid: 6,
		target_digest: Some([0x16; 32]),
		k: 106,
	};
	actual.push((
		"recovery_proved".to_owned(),
		hex(&postcard::to_stdvec(&WendelonRequest::Decision(
			crate::consensus::decisions::DecisionCommand::RecoveryProved {
				proof: fixture::sign_for_test(recovery, fixture::signer(1)),
			},
		))
		.unwrap()),
	));
	// Wendelon ADR 0108: the signed activation, an index receipt, and a
	// replacement record with its activation.
	let activation = crate::consensus::decisions::ActivationBody {
		token: 0x0108_0108_0108_0108_0108_0108_0108_0108,
		slot: 1,
		r_new: crate::consensus::decisions::instances::ALLOCATED_BASE,
		birth: [0x08; 16],
		store: crate::consensus::StoreIncarnation::from_raw(0x0108),
		recovered_gid: 6,
		k: 106,
		through: 180,
		certified: 7,
	};
	actual.push((
		"activate_replacement".to_owned(),
		hex(&postcard::to_stdvec(&WendelonRequest::Decision(
			crate::consensus::decisions::DecisionCommand::ActivateReplacement {
				proof: fixture::sign_for_test(activation, fixture::signer(1)),
			},
		))
		.unwrap()),
	));
	let receipt = crate::consensus::decisions::IndexReceiptBody {
		tenant: crate::schema::TenantId::new(7),
		index: crate::schema::IndexObjectId::new([0x08; 16]),
		phase: crate::consensus::decisions::IndexReceiptPhase::Backfilled,
		signer: fixture::signer(1),
	};
	actual.push((
		"index_receipt".to_owned(),
		hex(&postcard::to_stdvec(&WendelonRequest::Decision(
			crate::consensus::decisions::DecisionCommand::IndexReceipt {
				proof: fixture::sign_for_test(receipt, fixture::signer(1)),
			},
		))
		.unwrap()),
	));
	actual.push((
		"activated_replacement".to_owned(),
		hex(
			&postcard::to_stdvec(&crate::consensus::decisions::instances::Replacement {
				slot: 1,
				r_old: 1,
				pre_voters: [1, 2, 3].into_iter().collect(),
				phase: crate::consensus::decisions::instances::ReplacementPhase::Done {
					r_new: crate::consensus::decisions::instances::ALLOCATED_BASE,
				},
				promoted: Some(crate::consensus::decisions::instances::Promotion {
					expected: fixture::inc(1),
					incarnation: crate::consensus::StoreIncarnation::from_raw(0x0108),
				}),
				recovered: Some(crate::consensus::decisions::instances::Recovery {
					gid: 6,
					target_digest: Some([0x16; 32]),
					k: 106,
				}),
				activated: Some(crate::consensus::decisions::instances::Activation {
					incarnation: crate::consensus::StoreIncarnation::from_raw(0x0108),
					through: 180,
					certified: 7,
				}),
			})
			.unwrap(),
		),
	));
	// Wendelon ADR 0112: a delete statement of three rows (two matched, in two
	// children; one already deleted) and a noop of two rows.
	for (name, bytes) in golden_delete_statement() {
		actual.push((name.to_owned(), hex(&bytes)));
	}
	// Wendelon ADR 0118 D1: a parent concluded by its table's drop Prepare
	// (production from this format on).
	actual.push((
		"parent_refused_table_dropped".to_owned(),
		hex(
			&postcard::to_stdvec(&crate::consensus::decisions::ParentStatus::Refused {
				cause: crate::consensus::decisions::RefusalCause::TableDropped,
				dec: 118,
			})
			.unwrap(),
		),
	));
	// Wendelon ADR 0120: the CTAS chain commands, and a chunk concluded by its
	// chain's abandonment.
	for (name, command) in golden_ctas_commands() {
		actual.push((
			name.to_owned(),
			hex(&postcard::to_stdvec(&WendelonRequest::Decision(command)).unwrap()),
		));
	}
	actual.push((
		"parent_refused_ctas_abandoned".to_owned(),
		hex(
			&postcard::to_stdvec(&crate::consensus::decisions::ParentStatus::Refused {
				cause: crate::consensus::decisions::RefusalCause::CtasAbandoned,
				dec: 120,
			})
			.unwrap(),
		),
	));
	// Wendelon ADR 0121: the bind command, and a target, report and certificate
	// carrying a retention cut.
	for (name, bytes) in golden_retention_shapes() {
		actual.push((name.to_owned(), hex(&bytes)));
	}
	let moved: Vec<String> = actual
		.iter()
		.filter(|(name, hex)| stored.get(name) != Some(hex))
		.map(|(name, hex)| format!("{name}={hex}"))
		.collect();
	assert!(
		moved.is_empty(),
		"format-34 bodies moved or are missing; expected lines:\n{}",
		moved.join("\n")
	);
	assert_eq!(stored.len(), actual.len());

	let decoded = decode_state_body(DecisionMode::ParentDecisions, &body).unwrap();
	assert_eq!(decoded.decisions, state.decisions);
	assert_eq!(decoded.leases, state.leases);
	assert_eq!(decoded.epoch_history, state.epoch_history);
	let decisions = decoded.decisions.as_ref().unwrap();
	// ADR 0720: the golden history admitted writes, so its state is sealed, and
	// only the decision state differs from the format-16 corpus: by exactly the
	// appended seal.
	assert!(decisions.sealed().is_some());
	// Wendelon ADR 0121: against the format-33 corpus only the decision state moved,
	// by exactly its appended retention binding (the golden history binds the
	// Disabled window: `Some(0)`); the bind command and the cut's shapes were added.
	assert_eq!(
		decisions.retention(),
		Some(crate::consensus::decisions::RetentionBinding { window_ms: 0 })
	);
	const ADDED_0121: [&str; 4] = [
		"bind_retention",
		"propose_target_retention",
		"report_retention",
		"certificate_retention",
	];
	let retained_v33 = parse(PARENT_DECISIONS_V33_MANIFEST);
	assert_eq!(stored.len(), retained_v33.len() + ADDED_0121.len());
	for (name, hex) in &retained_v33 {
		if name == "decisions_golden_history" {
			assert_eq!(stored.get(name), Some(&format!("{hex}0100")), "{name}");
		} else {
			assert_eq!(stored.get(name), Some(hex), "{name}");
		}
	}
	for name in ADDED_0121 {
		assert!(stored.contains_key(name), "{name}");
	}
	// Wendelon ADR 0120: against the format-32 corpus only the decision state
	// moved, by exactly its appended empty CTAS part (four empty maps and two zero
	// counters); the CTAS commands and a chunk refused by abandonment were added.
	const ADDED_0120: [&str; 8] = [
		"ctas_begin",
		"ctas_renew",
		"ctas_recover",
		"ctas_publish",
		"ctas_conclude",
		"ctas_abandon",
		"ctas_prune",
		"parent_refused_ctas_abandoned",
	];
	let retained_v32 = parse(PARENT_DECISIONS_V32_MANIFEST);
	assert_eq!(retained_v33.len(), retained_v32.len() + ADDED_0120.len());
	for (name, hex) in &retained_v32 {
		if name == "decisions_golden_history" {
			assert_eq!(
				retained_v33.get(name),
				Some(&format!("{hex}000000000000")),
				"{name}"
			);
		} else {
			assert_eq!(retained_v33.get(name), Some(hex), "{name}");
		}
	}
	for name in ADDED_0120 {
		assert!(retained_v33.contains_key(name), "{name}");
	}
	// Wendelon ADR 0119: the format-32 corpus is the format-31 corpus, byte for byte.
	assert_eq!(retained_v32, parse(PARENT_DECISIONS_V31_MANIFEST));
	// Wendelon ADR 0118: against the format-30 corpus no body moved; a parent
	// refused by its table's drop was added.
	const ADDED_0118: [&str; 1] = ["parent_refused_table_dropped"];
	let retained_v30 = parse(PARENT_DECISIONS_V30_MANIFEST);
	assert_eq!(retained_v32.len(), retained_v30.len() + ADDED_0118.len());
	for (name, hex) in &retained_v30 {
		assert_eq!(retained_v32.get(name), Some(hex), "{name}");
	}
	for name in ADDED_0118 {
		assert!(retained_v32.contains_key(name), "{name}");
	}
	// Wendelon ADR 0113: the format-30 corpus is the format-29 corpus, byte for
	// byte.
	assert_eq!(retained_v30, parse(PARENT_DECISIONS_V29_MANIFEST));
	// Wendelon ADR 0112: against the format-28 corpus no body moved; a delete
	// statement's claim and noop were added.
	const ADDED_0112: [&str; 2] = ["delete_statement_claim", "delete_statement_noop"];
	let retained_v28 = parse(PARENT_DECISIONS_V28_MANIFEST);
	assert_eq!(retained_v30.len(), retained_v28.len() + ADDED_0112.len());
	for (name, hex) in &retained_v28 {
		assert_eq!(retained_v30.get(name), Some(hex), "{name}");
	}
	for name in ADDED_0112 {
		assert!(retained_v30.contains_key(name), "{name}");
	}
	// Wendelon ADR 0108: against the format-27 corpus three bodies moved (the
	// decision state gained the index receipts, the lease batch's grants carry
	// the holder's store, the promoted replacement record gained its
	// activation) and the activation, the index receipt and an activated
	// replacement record were added.
	const MOVED_0108: [&str; 3] = [
		"decisions_golden_history",
		"acquire_leases_0",
		"promoted_replacement",
	];
	let retained_v27 = parse(PARENT_DECISIONS_V27_MANIFEST);
	assert_eq!(retained_v28.len(), retained_v27.len() + 3);
	for (name, hex) in &retained_v27 {
		if MOVED_0108.contains(&name.as_str()) {
			assert_ne!(retained_v28.get(name), Some(hex), "{name}");
		} else {
			assert_eq!(retained_v28.get(name), Some(hex), "{name}");
		}
	}
	// Wendelon ADR 0106: against the format-26 corpus one body moved (the
	// promoted replacement record gained its recovery) and the recovery
	// statement was added.
	const MOVED_0106: [&str; 1] = ["promoted_replacement"];
	let retained_v26 = parse(PARENT_DECISIONS_V26_MANIFEST);
	assert_eq!(retained_v27.len(), retained_v26.len() + 1);
	for (name, hex) in &retained_v26 {
		if MOVED_0106.contains(&name.as_str()) {
			assert_ne!(retained_v27.get(name), Some(hex), "{name}");
		} else {
			assert_eq!(retained_v27.get(name), Some(hex), "{name}");
		}
	}
	// Wendelon ADR 0105: against the format-25 corpus no body moved; the
	// promotion request and a promoted replacement record were added.
	let retained_v25 = parse(PARENT_DECISIONS_V25_MANIFEST);
	assert_eq!(retained_v26.len(), retained_v25.len() + 2);
	for (name, hex) in &retained_v25 {
		assert_eq!(retained_v26.get(name), Some(hex), "{name}");
	}
	// ADR 0104 (wendelon): against the format-24 corpus exactly one body moved,
	// the decision state (the Raft instance records), and the three instance
	// commands were added. Every other body is unchanged, byte for byte.
	const MOVED_0104: [&str; 1] = ["decisions_golden_history"];
	let retained_v24 = parse(PARENT_DECISIONS_V24_MANIFEST);
	assert_eq!(retained_v25.len(), retained_v24.len() + 3);
	for (name, hex) in &retained_v24 {
		if MOVED_0104.contains(&name.as_str()) {
			assert_ne!(retained_v25.get(name), Some(hex), "{name}");
		} else {
			assert_eq!(retained_v25.get(name), Some(hex), "{name}");
		}
	}
	// ADR 0736: against the format-23 corpus exactly one body moved, the
	// decision state (the backup hold), and the two hold commands were added.
	// Every other body is unchanged, byte for byte.
	const MOVED: [&str; 1] = ["decisions_golden_history"];
	let retained_v23 = parse(PARENT_DECISIONS_V23_MANIFEST);
	assert_eq!(retained_v24.len(), retained_v23.len() + 2);
	for (name, hex) in &retained_v23 {
		if MOVED.contains(&name.as_str()) {
			assert_ne!(retained_v24.get(name), Some(hex), "{name}");
		} else {
			assert_eq!(retained_v24.get(name), Some(hex), "{name}");
		}
	}
	// ADR 0729: against the format-18 corpus, exactly two bodies moved: the
	// decision state (lineage fields) and the lease batch (its stamp, and a
	// generation per grant). Every decision, certified and owner-signed body
	// is unchanged, byte for byte.
	const MOVED_0729: [&str; 2] = ["decisions_golden_history", "acquire_leases_0"];
	let retained_v18 = parse(PARENT_DECISIONS_V18_MANIFEST);
	assert_eq!(retained_v23.len(), retained_v18.len());
	for (name, hex) in &retained_v18 {
		if MOVED_0729.contains(&name.as_str()) {
			assert_ne!(retained_v23.get(name), Some(hex), "{name}");
		} else {
			assert_eq!(retained_v23.get(name), Some(hex), "{name}");
		}
	}
	// ADR 0725: every format-17 body is unchanged in format 18, and only the
	// three owner-signed operations are new there.
	let retained_v17 = parse(PARENT_DECISIONS_V17_MANIFEST);
	for (name, hex) in &retained_v17 {
		assert_eq!(retained_v18.get(name), Some(hex), "{name}");
	}
	assert_eq!(retained_v18.len(), retained_v17.len() + 3);
	// ADR 0720 appended the seal to the decision state and changed nothing else.
	let retained_v16 = parse(PARENT_DECISIONS_V16_MANIFEST);
	assert_eq!(retained_v17.len(), retained_v16.len());
	for (name, hex) in &retained_v16 {
		if name == "decisions_golden_history" {
			let later = &retained_v17[name];
			assert!(later.starts_with(hex.as_str()) && later.len() > hex.len());
		} else {
			assert_eq!(retained_v17.get(name), Some(hex), "{name}");
		}
	}
	assert_eq!(decisions.retained_bodies(), 2);
	assert_eq!(decisions.retry_keys(), 1);
	assert!(decisions.floor_ms() > 0 && decisions.next_generation() > 2);
	// Today cannot read it as its own body.
	assert!(decode_state_body(DecisionMode::Today, &body).is_err());
	// ADR 0701 appended a variant: every retained format-13 body is unchanged.
	// (The decision state itself is compared with format 16 above: ADR 0720
	// appended the seal to it.)
	let retained = parse(PARENT_DECISIONS_V13_MANIFEST);
	for (name, hex) in &retained {
		if !MOVED_0729.contains(&name.as_str()) {
			assert_eq!(retained_v23.get(name), Some(hex), "{name}");
		}
	}
	// ADR 0704 changed no decision or lease body: every format-14 body is
	// unchanged, and only the two certified slab bodies are new.
	let retained_v14 = parse(PARENT_DECISIONS_V14_MANIFEST);
	for (name, hex) in &retained_v14 {
		if !MOVED_0729.contains(&name.as_str()) {
			assert_eq!(retained_v23.get(name), Some(hex), "{name}");
		}
	}
	assert_eq!(retained_v17.len(), retained_v14.len() + 2);
	assert_eq!(retained_v14.len(), retained.len() + 1);
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
	// stand-in signatures; S3 wrote format 11, whose claims carry no operation;
	// S4b wrote 12, S5a to ADR 0700 13 and ADR 0701 to ADR 0703 14. None is
	// converted: each is refused before replay or decode, files unchanged. ADR
	// 0704 gave Today 15, not 9, so no authority ever reuses a retired number.
	// ADR 0720 retired 16, whose decision state records no sealed membership.
	// ADR 0725 retired 17, whose logs may hold bare slab operations that applied.
	for retired in [9u8, 10, 11, 12, 13, 14, 16, 17] {
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
			apply_entry_to_state(&mut state, entry).unwrap();
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
		apply_entry_to_state(&mut resumed, entry).unwrap();
	}
	assert_eq!(resumed.decisions, state.decisions);

	// Recovery starts each authority from its own state shape.
	let blanks: Vec<_> = (1..=3).map(blank).collect();
	let inner = recover_sm_inner(new, None, blanks.clone(), checkpoint(3), false).unwrap();
	assert_eq!(inner.state.decisions, Some(Default::default()));
	let inner = recover_sm_inner(DecisionMode::Today, None, blanks, checkpoint(3), false).unwrap();
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

/// Wendelon ADR 0121 S1 baseline: the frozen Section 8 driver restates
/// `InheritCluster` at generation ZERO (`toron-tpch` `s7_measure/drive.rs`).
/// Under ParentDecisions that restatement keeps the ordinary contract: the first
/// one succeeds and advances the policy generation exactly as outside the mode
/// (`TableLifecycleCatalog::set_next_generation`), a stale repeat is refused and
/// changes nothing, and any other policy stays frozen.
#[test]
fn an_inherit_cluster_restatement_keeps_the_ordinary_contract_under_parent_decisions() {
	use crate::consensus::SetTableRetentionPolicy;
	use crate::schema::{PolicyGeneration, TableRetentionPolicy};
	let restate = |expected, new_policy| {
		WendelonRequest::SetTableRetentionPolicy(SetTableRetentionPolicy {
			tenant_id: fixture::TENANT,
			table_object_id: fixture::TABLE,
			expected_policy_generation: expected,
			new_policy,
			updated_at_hlc: crate::schema::HybridTimestamp {
				physical_ms: 42,
				logical: 1,
				node_id: 1,
			},
		})
	};
	// The same command outside the mode: the contract the mode must keep.
	let mut today = fixture::base(3);
	today.decisions = None;
	let (response, _) = apply_request_at_log_index(
		&mut today,
		restate(PolicyGeneration::ZERO, TableRetentionPolicy::InheritCluster),
		1,
		0,
	);
	assert!(
		matches!(response, WendelonResponse::Success),
		"{response:?}"
	);
	let generation = today
		.table_lifecycle
		.current_generation(fixture::TENANT, fixture::TABLE);
	assert_eq!(generation, PolicyGeneration::ZERO.next());

	let mut state = fixture::base(3);
	let (response, _) = apply_request_at_log_index(
		&mut state,
		restate(PolicyGeneration::ZERO, TableRetentionPolicy::InheritCluster),
		1,
		0,
	);
	assert!(
		matches!(response, WendelonResponse::Success),
		"the first InheritCluster restatement at ZERO is accepted under ParentDecisions: {response:?}"
	);
	assert_eq!(state.table_lifecycle, today.table_lifecycle);
	// A stale repeat is refused and changes nothing.
	let before = state.table_lifecycle.clone();
	let (response, _) = apply_request_at_log_index(
		&mut state,
		restate(PolicyGeneration::ZERO, TableRetentionPolicy::InheritCluster),
		2,
		0,
	);
	assert!(
		matches!(&response, WendelonResponse::Error(why) if why.contains("generation")),
		"{response:?}"
	);
	assert_eq!(state.table_lifecycle, before);
	// Any other policy stays frozen in S1.
	let (response, _) = apply_request_at_log_index(
		&mut state,
		restate(generation, TableRetentionPolicy::Disabled),
		3,
		0,
	);
	assert!(
		matches!(&response, WendelonResponse::Error(why) if why.contains("frozen")),
		"{response:?}"
	);
	assert_eq!(state.table_lifecycle, before);
}

/// Wendelon ADR 0121 R3: at the real snapshot boundary (the body a snapshot file
/// and an install decode through), a state is refused when a retention field
/// contradicts its binding or its applied clock, and accepted when consistent;
/// unbound genesis stays legal. Each refused state differs from a valid one in
/// its retention fields only.
#[test]
fn a_snapshot_whose_retention_cut_contradicts_its_binding_or_clock_is_refused() {
	use crate::consensus::decisions::generations::RetentionCut;
	let mode = DecisionMode::ParentDecisions;
	let cut = |window_ms, stamp_ms, floor_ms| RetentionCut {
		window_ms,
		stamp_ms,
		floor_ms,
	};
	let hlc = |physical_ms| crate::schema::HybridTimestamp {
		physical_ms,
		logical: 1,
		node_id: 1,
	};
	let round = |state: &SmState| decode_state_body(mode, &encode_state_body(mode, state).unwrap());
	let (w, clock) = (15_000, 1_000_000);
	let good = cut(w, clock, clock - w);
	assert!(
		round(&fixture::retention_state(None, 0, None, None, None)).is_ok(),
		"unbound genesis"
	);
	let valid =
		fixture::retention_state(Some(w), clock, Some(good), Some(hlc(clock - w)), Some(good));
	assert_eq!(
		round(&valid).unwrap().decisions,
		valid.decisions,
		"a consistent store"
	);
	let disabled = fixture::retention_state(
		Some(0),
		clock,
		Some(cut(0, 0, 0)),
		None,
		Some(cut(0, clock, 0)),
	);
	assert!(round(&disabled).is_ok(), "the Disabled binding");
	for (what, state) in [
		(
			"a certificate under an unbound store",
			fixture::retention_state(None, clock, Some(cut(0, 0, 0)), None, None),
		),
		(
			"a certificate of another window",
			fixture::retention_state(Some(w), clock, Some(cut(0, 0, 0)), None, None),
		),
		(
			"a floor above clock - window",
			fixture::retention_state(
				Some(w),
				clock,
				Some(cut(w, clock, clock - w + 1)),
				None,
				None,
			),
		),
		(
			"a stamp beyond the applied clock",
			fixture::retention_state(
				Some(w),
				clock,
				Some(cut(w, clock + 1, clock - w)),
				None,
				None,
			),
		),
		(
			"a floor under the Disabled binding",
			fixture::retention_state(Some(0), clock, Some(cut(0, 0, 1)), None, None),
		),
		(
			"a least visible row below the certified floor",
			fixture::retention_state(Some(w), clock, Some(good), Some(hlc(clock - w - 1)), None),
		),
		(
			"a pending target of another window",
			fixture::retention_state(Some(w), clock, Some(good), None, Some(cut(0, 0, 0))),
		),
		(
			"a pending floor below the certificate's",
			fixture::retention_state(
				Some(w),
				clock,
				Some(good),
				None,
				Some(cut(w, clock, clock - w - 1)),
			),
		),
	] {
		let error = round(&state).expect_err(what);
		assert!(
			error.contains("retention") || error.contains("decisions"),
			"{what}: {error}"
		);
	}
}

/// ADR 0699 S5a: the survivor and read policy is frozen in mode. A retention
/// change and a tenant delete are refused at apply and change nothing; CTAS, a
/// write path outside parent decisions, is refused too. (Wendelon ADR 0118 D1: a
/// table drop is the ParentDecisions Prepare now, covered by the drop tests.)
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
	// Wendelon ADR 0120: typed, so a caller falls back to the decision chain.
	assert_eq!(
		response,
		WendelonResponse::Error(format!(
			"{}{}",
			crate::schema::CTAS_SAGA_REJECT_PREFIX,
			crate::schema::CtasSagaReject::WrongDecisionMode.code()
		))
	);
	assert!(
		state
			.ctas_sagas
			.get(
				fixture::TENANT,
				crate::schema::CtasOperationId::new(operation)
			)
			.is_none(),
		"a saga was recorded"
	);
}

// ============================================================
//  ADR 0104 (wendelon): BIRTH, IDENTITY AND VOTE IN ONE PAGE
// ============================================================

use crate::consensus::decisions::instances::{Origin, RaftIdentity, Role};
use openraft::storage::RaftLogStorage;

const IDENTITY: RaftIdentity = RaftIdentity {
	instance: 7,
	origin: Origin::Join,
	role: Role::Full,
};

fn refused_as(result: std::io::Result<WendelonLogStore>, reason: &str) {
	match result {
		Ok(_) => panic!("the open was not refused ({reason})"),
		Err(error) => {
			assert_eq!(error.kind(), ErrorKind::InvalidData, "{error}");
			assert!(error.to_string().starts_with(reason), "{error}");
		}
	}
}

#[tokio::test]
async fn a_new_store_is_born_once_and_stays_fresh_until_its_identity() {
	let dir = TempDir::new("adr0104-born");
	let new = DecisionMode::ParentDecisions;
	let store = open_store(&dir, new).await.unwrap();
	let birth = store.birth().await.expect("born at its first open");
	assert_eq!(store.identity().await, None);
	assert!(store.is_pristine().await);
	assert_eq!(
		store.bootstrap_mode().await,
		DurableBootstrapMode::FreshBootstrap
	);
	drop(store);
	// A born store reopens with the same birth, still fresh.
	let store = open_store(&dir, new).await.unwrap();
	assert_eq!(store.birth().await, Some(birth));
	assert_eq!(
		store.bootstrap_mode().await,
		DurableBootstrapMode::FreshBootstrap
	);
	// Today has no page.
	let today = TempDir::new("adr0104-today");
	assert_eq!(
		open_store(&today, DecisionMode::Today)
			.await
			.unwrap()
			.birth()
			.await,
		None
	);
}

#[tokio::test]
async fn the_identity_is_written_once_and_kept_with_every_vote() {
	let dir = TempDir::new("adr0104-identity");
	let new = DecisionMode::ParentDecisions;
	let store = open_store(&dir, new).await.unwrap();
	let birth = store.birth().await.unwrap();
	store.write_identity(IDENTITY).await.unwrap();
	store.write_identity(IDENTITY).await.unwrap();
	let other = RaftIdentity {
		instance: 8,
		..IDENTITY
	};
	assert_eq!(
		store.write_identity(other).await.unwrap_err().kind(),
		ErrorKind::AlreadyExists
	);
	// Identified, with nothing Raft-durable yet: still fresh, under this identity.
	assert!(store.is_pristine().await);
	drop(store);
	let mut store = open_store(&dir, new).await.unwrap();
	assert_eq!(store.identity().await, Some(IDENTITY));
	assert_eq!(
		store.bootstrap_mode().await,
		DurableBootstrapMode::FreshBootstrap
	);
	let vote = Vote::new(3, 7);
	store.save_vote(&vote).await.unwrap();
	store.blocking_append(vec![blank(0)]).await.unwrap();
	drop(store);
	let mut store = open_store(&dir, new).await.unwrap();
	assert_eq!(store.birth().await, Some(birth));
	assert_eq!(store.identity().await, Some(IDENTITY));
	assert_eq!(store.read_vote().await.unwrap(), Some(vote));
	assert_eq!(store.bootstrap_mode().await, DurableBootstrapMode::Recovery);
}

#[tokio::test]
async fn an_identified_store_with_an_empty_log_keeps_its_vote_and_is_not_fresh() {
	let dir = TempDir::new("adr0104-identified-empty");
	let new = DecisionMode::ParentDecisions;
	let mut store = open_store(&dir, new).await.unwrap();
	store.write_identity(IDENTITY).await.unwrap();
	let vote = Vote::new(2, 9);
	store.save_vote(&vote).await.unwrap();
	drop(store);
	let mut store = open_store(&dir, new).await.unwrap();
	assert_eq!(store.read_vote().await.unwrap(), Some(vote));
	assert_eq!(store.identity().await, Some(IDENTITY));
	assert_eq!(store.bootstrap_mode().await, DurableBootstrapMode::Recovery);
	assert!(!store.is_pristine().await);
}

#[tokio::test]
async fn known_loss_of_either_half_fails_closed_and_changes_nothing() {
	let new = DecisionMode::ParentDecisions;
	// The identity page survives its control log.
	let dir = TempDir::new("adr0104-no-log");
	let store = open_store(&dir, new).await.unwrap();
	store.write_identity(IDENTITY).await.unwrap();
	drop(store);
	for segment in discover_all_wal_segments(
		&crate::storage::BlockingFileOps,
		&dir.to_path_buf().join("control.wal"),
	)
	.await
	.unwrap()
	{
		std::fs::remove_file(segment.path).unwrap();
	}
	let before = file_bytes(&dir);
	refused_as(open_store(&dir, new).await, "RaftLogMissing");
	assert_eq!(file_bytes(&dir), before);

	// Control state survives its identity page.
	let dir = TempDir::new("adr0104-no-page");
	let mut store = open_store(&dir, new).await.unwrap();
	store.write_identity(IDENTITY).await.unwrap();
	store
		.blocking_append(vec![blank(0), blank(1)])
		.await
		.unwrap();
	drop(store);
	std::fs::remove_file(dir.to_path_buf().join("vote.wnd")).unwrap();
	let before = file_bytes(&dir);
	refused_as(open_store(&dir, new).await, "RaftIdentityMissing");
	assert_eq!(file_bytes(&dir), before);

	// A holder alone (a crash before the birth page) is a fresh store.
	let dir = TempDir::new("adr0104-holder-only");
	drop(open_store(&dir, new).await.unwrap());
	std::fs::remove_file(dir.to_path_buf().join("vote.wnd")).unwrap();
	let store = open_store(&dir, new).await.unwrap();
	assert!(store.is_pristine().await);
	assert!(store.birth().await.is_some());
}

// ============================================================
//  ADR 0104 (wendelon), control 9: APPLIED BY SLOT, RAW LOG IDS
// ============================================================

use crate::consensus::decisions::instances::ALLOCATED_BASE;
use crate::schema::secondary_index::SecondaryIndexState;
use crate::schema::{IndexObjectId, NodeId};
use openraft::storage::RaftStateMachine;

const APPLIED: u64 = 10;
/// An allocated instance with no record.
const UNRECORDED: u32 = ALLOCATED_BASE + 1;

/// Slot 3 is being replaced by `ALLOCATED_BASE`, which the state records; one
/// index has its backfill from slots 1, 2 and 3 and is still building, under
/// voters that also named a fourth.
fn replacing() -> (SmState, IndexObjectId) {
	let mut state = fixture::reserved();
	let index = crate::schema::SecondaryIndexDescriptor::for_table_column(
		IndexObjectId::new([0x9f; 16]),
		"by_v",
		&fixture::fixture_table(),
		"v",
	)
	.unwrap();
	let id = index.index_object_id;
	let schemas = state.table_schemas.clone();
	state
		.secondary_indexes
		.create(fixture::TENANT, index, &schemas)
		.unwrap();
	for voter in 1..=3 {
		state
			.secondary_indexes
			.mark_backfill_complete(
				fixture::TENANT,
				id,
				NodeId {
					id: voter,
					_reserved: 0,
				},
				&[1, 2, 3, 4].into_iter().collect(),
			)
			.unwrap();
	}
	state.last_applied_log_id = Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), APPLIED));
	assert!(!ready(&state, id));
	(state, id)
}

fn ready(state: &SmState, id: IndexObjectId) -> bool {
	state
		.secondary_indexes
		.for_tenant(fixture::TENANT)
		.iter()
		.any(|row| row.descriptor.index_object_id == id && row.state == SecondaryIndexState::Ready)
}

/// A membership entry, appended by slot 1 in a later term than `authored`'s,
/// whose voters are 1, 2 and `third`.
fn membership(index: u64, third: u32) -> Entry<WendelonTypeConfig> {
	let voters: BTreeSet<u32> = [1, 2, third].into_iter().collect();
	let nodes: BTreeMap<u32, openraft::BasicNode> = voters
		.iter()
		.map(|id| {
			(
				*id,
				openraft::BasicNode {
					addr: format!("127.0.0.1:{}", 7000 + u64::from(*id)),
				},
			)
		})
		.collect();
	Entry {
		log_id: LogId::new(openraft::CommittedLeaderId::new(3, 1), index),
		payload: EntryPayload::Membership(openraft::Membership::new(vec![voters], nodes)),
	}
}

/// An entry appended by `author`: slot 1's registration, which
/// ParentDecisions judges by the node it names, whoever appends it.
fn authored(index: u64, author: u32) -> Entry<WendelonTypeConfig> {
	Entry {
		log_id: LogId::new(openraft::CommittedLeaderId::new(2, author), index),
		payload: EntryPayload::Normal(WendelonRequest::RegisterIncarnation(
			crate::consensus::RegisterIncarnation {
				node: 1,
				incarnation: fixture::inc(1),
				supersedes: None,
			},
		)),
	}
}

/// Recovery's replay of `entries` over a snapshot of `state`.
fn recovered(state: SmState, entries: Vec<Entry<WendelonTypeConfig>>) -> std::io::Result<SmInner> {
	let last = state.last_applied_log_id;
	let commit = entries.last().map_or(APPLIED, |entry| entry.log_id.index);
	recover_sm_inner(
		DecisionMode::ParentDecisions,
		Some(LoadedSnapshot {
			last_included: last,
			held: HeldSnapshot {
				meta: openraft::SnapshotMeta {
					last_log_id: last,
					last_membership: state.last_membership.clone(),
					snapshot_id: "adr0104-control-9".into(),
				},
				path: PathBuf::new(),
			},
			state,
			artifact_hash: [0; 32],
			artifact_bytes: 0,
		}),
		entries,
		Some(RecoveryCheckpoint {
			commit_index: commit,
			e_safe_index: commit,
		}),
		false,
	)
}

/// A live state machine holding `state`.
async fn live(state: SmState) -> WendelonStateMachine {
	let machine = WendelonStateMachine::new();
	machine.seed_complete_snapshot_state_for_test(state).await;
	machine
}

/// An entry appended by a recorded allocated instance, and a membership naming
/// it, are applied as its slot at replay and at live apply alike: the index
/// whose backfill slots 1, 2 and 3 completed is promoted by voters 1, 2 and the
/// replacement. The log ids and the membership keep the raw instance.
#[tokio::test]
async fn a_recorded_instance_is_applied_as_its_slot_with_its_raw_log_id_kept() {
	let (state, index) = replacing();
	let entries = vec![
		authored(APPLIED + 1, ALLOCATED_BASE),
		membership(APPLIED + 2, ALLOCATED_BASE),
	];
	let raw_voters: BTreeSet<u32> = [1, 2, ALLOCATED_BASE].into_iter().collect();

	let replayed = recovered(state.clone(), entries.clone()).unwrap().state;
	let mut machine = live(state.clone()).await;
	RaftStateMachine::apply(&mut machine, entries.clone())
		.await
		.unwrap();
	let applied = machine.state_snapshot().await;
	for (what, result) in [("replay", &replayed), ("live apply", &applied)] {
		assert_eq!(
			result.last_applied_log_id,
			Some(entries[1].log_id),
			"{what}"
		);
		assert_eq!(
			result.last_membership.log_id(),
			&Some(entries[1].log_id),
			"{what}"
		);
		assert_eq!(
			result
				.last_membership
				.membership()
				.voter_ids()
				.collect::<BTreeSet<_>>(),
			raw_voters,
			"{what}: the membership keeps the raw instance"
		);
		assert!(ready(result, index), "{what}: voters counted by slot");
	}
	// The discriminating case: a third voter that is not slot 3 promotes nothing.
	let other = recovered(state, vec![membership(APPLIED + 1, 4)]).unwrap();
	assert!(!ready(&other.state, index));
}

/// An allocated instance with no record, as an entry's author or as a voter,
/// stops replay (the open fails) and live apply (a state machine error), and
/// changes nothing.
#[tokio::test]
async fn an_unrecorded_instance_stops_replay_and_live_apply_and_changes_nothing() {
	let (state, index) = replacing();
	for entry in [
		authored(APPLIED + 1, UNRECORDED),
		membership(APPLIED + 1, UNRECORDED),
	] {
		let error = recovered(state.clone(), vec![entry.clone()])
			.err()
			.expect("replay stops");
		assert_eq!(error.kind(), ErrorKind::InvalidData, "{error}");
		assert!(
			error
				.to_string()
				.contains(&format!("Raft instance {UNRECORDED} has no recorded slot")),
			"{error}"
		);
		let mut machine = live(state.clone()).await;
		let error = RaftStateMachine::apply(&mut machine, vec![entry])
			.await
			.err()
			.expect("live apply stops");
		assert!(
			error.to_string().contains("has no recorded slot"),
			"{error}"
		);
		let after = machine.state_snapshot().await;
		assert_eq!(after.last_applied_log_id, state.last_applied_log_id);
		assert_eq!(after.last_membership, state.last_membership);
		assert!(!ready(&after, index));
	}
}

/// Wendelon ADR 0112: the delete statement bodies pinned in the format-29
/// corpus: a matched claim of three rows in two children, and a noop of two.
fn golden_delete_statement() -> [(&'static str, Vec<u8>); 2] {
	use crate::consensus::decisions::{
		ChildClaim, Claim, DeleteOutcome, DeleteRequest, DeleteSelection, LineageRef, NoopBody,
		Operation, ParentKey, ParentNamespace, Signer,
	};
	use crate::schema::{HybridTimestamp, Identity, TableObjectId, TenantId};
	let hlc = |ms: u64| HybridTimestamp {
		physical_ms: ms,
		logical: 1,
		node_id: 2,
	};
	let signer = |node: u32| Signer {
		node,
		incarnation: crate::consensus::StoreIncarnation::from_raw(0x0112_0000 + u128::from(node)),
	};
	let table = TableObjectId::new([0x12; 16]);
	let key = ParentKey {
		tenant: TenantId::ZERO,
		namespace: ParentNamespace::Table(table),
		client_uuid: [0x01; 16],
		sequence_no: 112,
	};
	let request = DeleteRequest {
		table,
		schema_generation: 3,
		rows: vec![
			Identity::new([0x10; 32]),
			Identity::new([0x20; 32]),
			Identity::new([0x30; 32]),
		],
	};
	let child = |start: u64, holder: u32| ChildClaim {
		range_start: start,
		range_end: start,
		lineage: LineageRef {
			range_start: start - 7,
			generation: 4,
		},
		holder: signer(holder),
		plan: vec![
			signer(holder),
			signer(holder % 3 + 1),
			signer((holder + 1) % 3 + 1),
		],
		rows: 1,
		content: [holder as u8; 32],
	};
	let claim = Claim {
		key,
		request_digest: [0x0c; 32],
		commit_hlc: hlc(1_700_000_000_112),
		coordinator: 0,
		first_row_position: 1_000,
		children: vec![child(1_000, 1), child(9_000, 2)],
		operation: Operation::Delete {
			request: request.clone(),
			selection: DeleteSelection {
				cut: 111,
				outcomes: vec![
					DeleteOutcome::Deleted {
						winner_rsv: 50,
						winner_hlc: hlc(1_700_000_000_050),
					},
					DeleteOutcome::AlreadyDeleted {
						winner_rsv: 60,
						winner_hlc: hlc(1_700_000_000_060),
					},
					DeleteOutcome::Deleted {
						winner_rsv: 70,
						winner_hlc: hlc(1_700_000_000_070),
					},
				],
			},
		},
	};
	let noop = NoopBody {
		key: ParentKey {
			sequence_no: 113,
			..key
		},
		request_digest: [0x0d; 32],
		request: DeleteRequest {
			rows: request.rows[..2].to_vec(),
			..request
		},
		selection: DeleteSelection {
			cut: 112,
			outcomes: vec![
				DeleteOutcome::NotFound,
				DeleteOutcome::AlreadyDeleted {
					winner_rsv: 60,
					winner_hlc: hlc(1_700_000_000_060),
				},
			],
		},
		position: 1_000,
		coordinator: signer(1),
		lineage: LineageRef {
			range_start: 993,
			generation: 4,
		},
	};
	[
		(
			"delete_statement_claim",
			postcard::to_stdvec(&claim).unwrap(),
		),
		("delete_statement_noop", postcard::to_stdvec(&noop).unwrap()),
	]
}

/// Wendelon ADR 0114 stage (a): a snapshot taken BEFORE the drop Prepare, with
/// the table's Reserved parents live and the original state ADMITTED, decodes
/// through the production body codec. The admission is not serialized: the
/// decoded state does not admit until the test admits it explicitly. The derived
/// live-by-table bucket is rebuilt and validated. The same Prepare at the same
/// index then gives the original and the restored states equal decision states
/// and fences, both valid, with the table's bucket emptied and both parents
/// `Refused { TableDropped }` at that index.
#[test]
fn a_snapshot_before_the_drop_prepare_rebuilds_the_table_bucket_and_replays_alike() {
	use crate::consensus::decisions::{ParentStatus, RefusalCause};
	let mut state = fixture::base(3);
	fixture::grant_all(&mut state);
	let mut reserved = Vec::new();
	for sequence_no in [1, 2] {
		let claim = fixture::claim(
			fixture::key(fixture::T0, sequence_no),
			fixture::children(&state, 1),
			0,
		);
		let crate::consensus::decisions::DecisionResult::Reserved { reservation } =
			fixture::reserve(&mut state, &claim, fixture::T0)
		else {
			panic!("reserve");
		};
		reserved.push(reservation);
	}
	// Wendelon ADR 0118 D1: the decoded state replays the Prepare with no input
	// beyond its own bytes.
	let bytes = encode_state_body(DecisionMode::ParentDecisions, &state).unwrap();
	let mut restored = decode_state_body(DecisionMode::ParentDecisions, &bytes).unwrap();
	let live = |state: &SmState| {
		state
			.decisions
			.as_ref()
			.unwrap()
			.live_of_table(fixture::TENANT, fixture::TABLE)
	};
	assert_eq!(
		live(&restored),
		reserved,
		"the decoded state rebuilt the bucket"
	);
	restored.decisions.as_ref().unwrap().validate().unwrap();
	let drop_id = crate::integration::compute::DropOperationId {
		tenant_id: fixture::TENANT,
		table_object_id: fixture::TABLE,
		schema_generation: crate::integration::compute::SchemaGeneration::new(1),
	};
	let prepare = || {
		WendelonRequest::TableDropPrepare(crate::consensus::TableDropPrepareRequest::current(
			drop_id,
			drop_id.schema_generation,
		))
	};
	let at = fixture::peek_index() + 10;
	let original = apply_request_at_index_for_test(&mut state, prepare(), at);
	let replayed = apply_request_at_index_for_test(&mut restored, prepare(), at);
	assert!(
		matches!(original, WendelonResponse::Success),
		"{original:?}"
	);
	assert!(
		matches!(replayed, WendelonResponse::Success),
		"{replayed:?}"
	);
	for decided in [&state, &restored] {
		decided.decisions.as_ref().unwrap().validate().unwrap();
		assert!(live(decided).is_empty(), "the table's bucket is emptied");
		for reservation in &reserved {
			assert_eq!(
				decided
					.decisions
					.as_ref()
					.unwrap()
					.body(*reservation)
					.map(|record| record.status),
				Some(ParentStatus::Refused {
					cause: RefusalCause::TableDropped,
					dec: at,
				})
			);
		}
	}
	assert_eq!(
		postcard::to_stdvec(state.decisions.as_ref().unwrap()).unwrap(),
		postcard::to_stdvec(restored.decisions.as_ref().unwrap()).unwrap(),
		"the original and the restored decision states are equal"
	);
	assert_eq!(
		postcard::to_stdvec(&state.table_drop_fences).unwrap(),
		postcard::to_stdvec(&restored.table_drop_fences).unwrap(),
		"and their fences"
	);
}

// ============================================================
//  WENDELON ADR 0115: ADMISSION ACROSS RECOVERY AND REPLAY
// ============================================================

fn normal_entry(index: u64, request: WendelonRequest) -> Entry<WendelonTypeConfig> {
	Entry {
		log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
		payload: EntryPayload::Normal(request),
	}
}

fn loaded_snapshot(state: SmState, last: Option<LogId<u32>>) -> LoadedSnapshot {
	LoadedSnapshot {
		last_included: last,
		held: HeldSnapshot {
			meta: SnapshotMeta {
				last_log_id: last,
				last_membership: state.last_membership.clone(),
				snapshot_id: "adr0115".into(),
			},
			path: PathBuf::new(),
		},
		state,
		artifact_hash: [0; 32],
		artifact_bytes: 0,
	}
}

/// A fixture ParentDecisions state whose drop is finalized, with an excluding
/// (policy 2) target pending; returns it with its last applied index.
fn policy_two_pending_state() -> (SmState, u64) {
	use crate::consensus::decisions::{DECISION_EXPIRY_MS, DecisionCommand};
	let mut state = fixture::base(3);
	fixture::grant_all(&mut state);
	fixture::finalize_drop_of(&mut state, fixture::TABLE);
	fixture::decide(
		&mut state,
		DecisionCommand::AdvanceFloor {
			floor_ms: fixture::T0,
			stamp_ms: fixture::T0 + DECISION_EXPIRY_MS,
		},
	);
	let applied = fixture::peek_index() - 1;
	let target =
		crate::consensus::decisions::generations::next_target(&state, applied, None).unwrap();
	assert_eq!(target.survivor_policy, 2);
	fixture::decide(&mut state, DecisionCommand::ProposeTarget { target });
	let last = fixture::peek_index() - 1;
	assert!(
		state
			.decisions
			.as_ref()
			.unwrap()
			.generations()
			.pending
			.as_ref()
			.is_some_and(|pending| pending.target.survivor_policy == 2)
	);
	state.last_applied_log_id = Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), last));
	(state, last)
}

/// Wendelon ADR 0118 D1: the policy-2 state with its drop outcome removed. Its
/// pending target's digest is intact, but no outcome at its cut requires policy
/// 2, so every ParentDecisions store must refuse it.
fn unjustified_policy_two_state() -> (SmState, u64) {
	let (mut state, last) = policy_two_pending_state();
	state.table_drop_outcomes = Default::default();
	(state, last)
}

/// A snapshot already holding an accepted policy-2 pending target recovers on a
/// ParentDecisions store with no admission input (wendelon ADR 0118 D1). A
/// pending target whose policy disagrees with the outcomes at its cut is refused,
/// before any replay, even though its digest is internally correct.
#[test]
fn a_policy_two_snapshot_recovers_only_when_its_outcomes_require_it() {
	let (state, last) = policy_two_pending_state();
	let at = Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), last));
	let recovered = recover_sm_inner(
		DecisionMode::ParentDecisions,
		Some(loaded_snapshot(state.clone(), at)),
		Vec::new(),
		None,
		false,
	)
	.expect("a ParentDecisions store recovers it");
	assert_eq!(
		recovered
			.state
			.decisions
			.as_ref()
			.unwrap()
			.generations()
			.pending
			.as_ref()
			.map(|pending| pending.target.survivor_policy),
		Some(2)
	);
	// The same pending target, its digest intact, without the outcome its cut
	// relies on.
	let (mismatched, _) = unjustified_policy_two_state();
	let refused = recover_sm_inner(
		DecisionMode::ParentDecisions,
		Some(loaded_snapshot(mismatched, at)),
		Vec::new(),
		None,
		false,
	)
	.err()
	.expect("a policy the outcomes do not require is refused");
	assert_eq!(refused.kind(), ErrorKind::InvalidData);
	assert!(refused.to_string().contains("survivor policy"), "{refused}");
}

/// The replay contract (wendelon ADR 0118 D1: one rule, no admission input): a
/// policy-1 snapshot whose committed suffix first introduces the drop and the
/// policy-2 proposal recovers to the live state: equal pending target and digest,
/// outcomes, derived index and applied index.
#[test]
fn a_suffix_introducing_policy_two_replays_alike() {
	use crate::consensus::decisions::{DECISION_EXPIRY_MS, DecisionCommand};
	let mut live = fixture::base(3);
	fixture::grant_all(&mut live);
	let base_index = fixture::peek_index() - 1;
	let snapshot = {
		let mut snapshot = live.clone();
		snapshot.last_applied_log_id = Some(LogId::new(
			openraft::CommittedLeaderId::new(1, 1),
			base_index,
		));
		snapshot
	};
	let id = crate::integration::compute::DropOperationId {
		tenant_id: fixture::TENANT,
		table_object_id: fixture::TABLE,
		schema_generation: crate::integration::compute::SchemaGeneration::new(1),
	};
	let mut entries = Vec::new();
	let mut apply = |state: &mut SmState, request: WendelonRequest| {
		let index = fixture::peek_index();
		entries.push(normal_entry(index, request.clone()));
		fixture::apply_next(state, request)
	};
	let fence = fixture::peek_index();
	apply(
		&mut live,
		WendelonRequest::TableDropPrepare(crate::consensus::TableDropPrepareRequest::current(
			id,
			id.schema_generation,
		)),
	);
	apply(
		&mut live,
		WendelonRequest::TableDropLogicalFinalize(
			crate::consensus::TableDropLogicalFinalizeRequest {
				operation_id: id,
				fence_log_index: fence,
			},
		),
	);
	apply(
		&mut live,
		WendelonRequest::Decision(DecisionCommand::AdvanceFloor {
			floor_ms: fixture::T0,
			stamp_ms: fixture::T0 + DECISION_EXPIRY_MS,
		}),
	);
	let applied = fixture::peek_index() - 1;
	let target =
		crate::consensus::decisions::generations::next_target(&live, applied, None).unwrap();
	assert_eq!(target.survivor_policy, 2, "the suffix introduces policy 2");
	apply(
		&mut live,
		WendelonRequest::Decision(DecisionCommand::ProposeTarget { target }),
	);
	let last = entries.last().unwrap().log_id;
	let recovered = recover_sm_inner(
		DecisionMode::ParentDecisions,
		Some(loaded_snapshot(
			snapshot,
			Some(LogId::new(
				openraft::CommittedLeaderId::new(1, 1),
				base_index,
			)),
		)),
		entries,
		Some(RecoveryCheckpoint {
			commit_index: last.index,
			e_safe_index: last.index,
		}),
		false,
	)
	.expect("the suffix replays")
	.state;
	let pending = |state: &SmState| {
		state
			.decisions
			.as_ref()
			.unwrap()
			.generations()
			.pending
			.as_ref()
			.map(|pending| (pending.target.clone(), pending.digest))
	};
	assert!(pending(&live).is_some());
	assert_eq!(
		pending(&recovered),
		pending(&live),
		"pending target and digest"
	);
	assert_eq!(
		recovered.table_drop_outcomes, live.table_drop_outcomes,
		"outcomes"
	);
	assert_eq!(recovered.last_applied_log_id, Some(last), "applied index");
	assert!(!live.pd_finalize_indices.is_empty(), "premise: an index");
	assert_eq!(recovered.pd_finalize_indices, live.pd_finalize_indices);
}

/// The replay contract on a store whose outcomes do not require policy 2: a
/// committed `ProposeTarget(policy 2)` replays as a refusal (a valid apply
/// outcome) and the next valid entry applies: the open succeeds with no pending
/// target and the applied index past both.
#[test]
fn a_refused_policy_two_proposal_replays_as_a_refusal_without_its_outcome() {
	use crate::consensus::decisions::{DECISION_EXPIRY_MS, DecisionCommand};
	let (justified_state, _) = policy_two_pending_state();
	let target = justified_state
		.decisions
		.as_ref()
		.unwrap()
		.generations()
		.pending
		.as_ref()
		.unwrap()
		.target
		.clone();
	// No drop outcome: policy 2 is required at no cut.
	let mut unjustified = fixture::base(3);
	fixture::grant_all(&mut unjustified);
	let base_index = fixture::peek_index() - 1;
	unjustified.last_applied_log_id = Some(LogId::new(
		openraft::CommittedLeaderId::new(1, 1),
		base_index,
	));
	let entries = vec![
		normal_entry(
			base_index + 1,
			WendelonRequest::Decision(DecisionCommand::ProposeTarget { target }),
		),
		normal_entry(
			base_index + 2,
			WendelonRequest::Decision(DecisionCommand::AdvanceFloor {
				floor_ms: fixture::T0 + 1,
				stamp_ms: fixture::T0 + 1 + DECISION_EXPIRY_MS,
			}),
		),
	];
	// The live apply of the same entries on the same state.
	let mut live = unjustified.clone();
	for entry in &entries {
		let EntryPayload::Normal(request) = &entry.payload else {
			unreachable!()
		};
		apply_request_at_index_for_test(&mut live, request.clone(), entry.log_id.index);
	}
	let last = entries.last().unwrap().log_id;
	let recovered = recover_sm_inner(
		DecisionMode::ParentDecisions,
		Some(loaded_snapshot(
			unjustified,
			Some(LogId::new(
				openraft::CommittedLeaderId::new(1, 1),
				base_index,
			)),
		)),
		entries,
		Some(RecoveryCheckpoint {
			commit_index: last.index,
			e_safe_index: last.index,
		}),
		false,
	)
	.expect("a refused command still opens")
	.state;
	assert!(
		recovered
			.decisions
			.as_ref()
			.unwrap()
			.generations()
			.pending
			.is_none(),
		"the refused proposal left no pending target"
	);
	assert_eq!(
		recovered.last_applied_log_id,
		Some(last),
		"replay advanced past both"
	);
	assert_eq!(
		recovered.decisions, live.decisions,
		"live and recovered decisions"
	);
	assert_eq!(recovered.table_drop_outcomes, live.table_drop_outcomes);
	assert_eq!(recovered.pd_finalize_indices, live.pd_finalize_indices);
}

/// Install under one rule (wendelon ADR 0118 D1): a policy-2 snapshot whose
/// outcomes do not require policy 2 is refused by a ParentDecisions store's
/// install before publication (no current snapshot appears, the machine keeps
/// its state); the valid policy-2 snapshot installs on a ParentDecisions store
/// with no admission input, and its derived index is rebuilt from its outcomes.
#[tokio::test]
async fn a_policy_two_snapshot_installs_only_when_its_outcomes_require_it() {
	let build = |source_sm: &mut WendelonStateMachine| {
		let source_sm = source_sm.clone();
		async move {
			let mut source_sm = source_sm;
			let mut builder =
				<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
					&mut source_sm,
				)
				.await;
			builder.build_snapshot().await.unwrap()
		}
	};
	let source = |tag: &'static str, state: SmState| async move {
		let dir = TempDir::new(tag);
		let store = open_store(&dir, DecisionMode::ParentDecisions)
			.await
			.unwrap();
		let sm = store.recovered_state_machine().await.unwrap();
		sm.seed_complete_snapshot_state_for_test(state).await;
		(dir, store, sm)
	};
	let (unjustified, _) = unjustified_policy_two_state();
	let (_unjustified_dir, _unjustified_store, mut unjustified_sm) =
		source("adr0118-install-source-unjustified", unjustified).await;
	let (state, _) = policy_two_pending_state();
	let (_source_dir, _source_store, mut source_sm) = source("adr0118-install-source", state).await;

	// Refused before publication.
	let target_dir = TempDir::new("adr0118-install-refusing");
	let target_store = open_store(&target_dir, DecisionMode::ParentDecisions)
		.await
		.unwrap();
	let mut target_sm = target_store.recovered_state_machine().await.unwrap();
	// A durable policy-1 snapshot of its own is current before the install.
	let own = {
		let mut own = fixture::base(3);
		fixture::grant_all(&mut own);
		own.last_applied_log_id = Some(LogId::new(
			openraft::CommittedLeaderId::new(1, 1),
			fixture::peek_index() - 1,
		));
		own
	};
	target_sm.seed_complete_snapshot_state_for_test(own).await;
	build(&mut target_sm).await;
	let before = target_sm.state_snapshot().await.last_applied_log_id;
	let current = target_dir.to_path_buf().join("snapshot.wnd");
	let current_before = std::fs::read(&current).ok();
	assert!(
		current_before.is_some(),
		"premise: a durable snapshot is current"
	);
	let built = build(&mut unjustified_sm).await;
	let err = <WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
		&mut target_sm,
		&built.meta,
		built.snapshot,
	)
	.await
	.expect_err("a policy-2 snapshot its outcomes do not require is refused");
	assert!(err.to_string().contains("survivor policy"), "{err}");
	assert_eq!(
		std::fs::read(&current).ok(),
		current_before,
		"nothing published"
	);
	assert_eq!(target_sm.state_snapshot().await.last_applied_log_id, before);

	// Installed, with no admission input.
	let installing_dir = TempDir::new("adr0118-install-installing");
	let installing_store = open_store(&installing_dir, DecisionMode::ParentDecisions)
		.await
		.unwrap();
	let mut installing_sm = installing_store.recovered_state_machine().await.unwrap();
	let built = build(&mut source_sm).await;
	<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::install_snapshot(
		&mut installing_sm,
		&built.meta,
		built.snapshot,
	)
	.await
	.expect("a ParentDecisions store installs it");
	let installed = installing_sm.state_snapshot().await;
	assert_eq!(
		installed
			.decisions
			.as_ref()
			.unwrap()
			.generations()
			.pending
			.as_ref()
			.map(|pending| pending.target.survivor_policy),
		Some(2)
	);
	assert!(
		!installed.pd_finalize_indices.is_empty(),
		"the derived index is rebuilt at install"
	);
	assert_eq!(
		installed.pd_finalize_indices,
		installed.derived_pd_finalize_indices()
	);
}

/// Opens `dir`'s control store through the constructor the nodes use.
async fn open_as_node(
	dir: &TempDir,
	owner: &DataDirectoryOwner,
) -> std::io::Result<WendelonLogStore> {
	let io = crate::storage::io_uring::spawn_io_thread(64)
		.unwrap()
		.with_data_directory_owner(owner.clone());
	WendelonLogStore::open_with_retention_and_owner(
		dir.to_path_buf().join("control.wal"),
		dir.to_path_buf().join("vote.wnd"),
		dir.to_path_buf().join("snapshot.wnd"),
		io,
		RetentionGuard::new(crate::consensus::ControlRetention::Compacting),
		owner.clone(),
		DecisionMode::ParentDecisions,
	)
	.await
}

/// Every regular file under `dir`, by relative path, with its bytes.
fn durable_files(dir: &std::path::Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
	let mut files = std::collections::BTreeMap::new();
	let mut pending = vec![dir.to_path_buf()];
	while let Some(at) = pending.pop() {
		for entry in std::fs::read_dir(&at).unwrap() {
			let path = entry.unwrap().path();
			if path.is_dir() {
				pending.push(path);
			} else {
				files.insert(
					path.strip_prefix(dir).unwrap().to_path_buf(),
					std::fs::read(&path).unwrap(),
				);
			}
		}
	}
	files
}

/// A durable ParentDecisions store written through the constructor the nodes
/// use: a snapshot of `state` (applied through `last`), one log entry after it,
/// and a torn tail on its last WAL segment that an open would truncate. Returns
/// the directory, its owner, that segment and the segment's untorn bytes.
async fn torn_store_holding(
	tag: &'static str,
	state: SmState,
	last: u64,
) -> (TempDir, DataDirectoryOwner, PathBuf, Vec<u8>) {
	let dir = TempDir::new(tag);
	let owner = DataDirectoryOwner::acquire_or_create(dir.to_path_buf()).unwrap();
	{
		let store = open_as_node(&dir, &owner).await.unwrap();
		store.write_identity(IDENTITY).await.unwrap();
		// The log the seeded state covers, so the snapshot sits on a real log.
		let mut store = store;
		store.blocking_append((0..=last).map(blank)).await.unwrap();
		let mut sm = store.recovered_state_machine().await.unwrap();
		sm.seed_complete_snapshot_state_for_test(state).await;
		let mut builder =
			<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::get_snapshot_builder(
				&mut sm,
			)
			.await;
		builder.build_snapshot().await.unwrap();
		// A log entry after the snapshot, so the WAL tail is a real segment.
		store.blocking_append([blank(last + 1)]).await.unwrap();
	}
	assert!(
		dir.to_path_buf().join("snapshot.wnd").exists(),
		"premise: a durable snapshot"
	);
	// A torn tail on the last WAL segment: a partial copy of its first page.
	let segment = durable_files(&dir.to_path_buf())
		.into_keys()
		.filter(|path| path.to_string_lossy().starts_with("control.wal"))
		.max()
		.expect("premise: a WAL segment");
	let segment = dir.to_path_buf().join(segment);
	let whole = std::fs::read(&segment).unwrap();
	assert!(whole.len() >= 128, "premise: a WAL page");
	{
		let mut file = std::fs::OpenOptions::new()
			.append(true)
			.open(&segment)
			.unwrap();
		file.write_all(&whole[..128]).unwrap();
		file.sync_all().unwrap();
	}
	(dir, owner, segment, whole)
}

/// The durable cold open, through the constructor the nodes use (wendelon ADR
/// 0118 D1). A store holding a policy-2 snapshot its outcomes do not require,
/// and a torn WAL tail the open would truncate, refuses in the open itself with
/// InvalidData, and every durable byte is kept. The same layout holding the
/// valid policy-2 snapshot opens with no admission input: the tail is truncated
/// as usual and the recovered state holds the pending policy-2 target.
#[tokio::test]
async fn a_cold_open_refuses_an_unjustified_policy_two_snapshot_before_any_effect() {
	let pending_of = |state: &SmState| {
		state
			.decisions
			.as_ref()
			.unwrap()
			.generations()
			.pending
			.as_ref()
			.map(|pending| (pending.target.clone(), pending.digest))
	};

	let (unjustified, last) = unjustified_policy_two_state();
	let (dir, owner, _, _) =
		torn_store_holding("adr0118-cold-open-unjustified", unjustified, last).await;
	let before = durable_files(&dir.to_path_buf());
	let refused = open_as_node(&dir, &owner)
		.await
		.err()
		.expect("the open of an unjustified policy-2 snapshot refuses");
	assert_eq!(refused.kind(), ErrorKind::InvalidData, "{refused}");
	assert!(refused.to_string().contains("survivor policy"), "{refused}");
	assert_eq!(
		durable_files(&dir.to_path_buf()),
		before,
		"no durable byte changed: no truncation, holder completion or purge"
	);

	let (state, last) = policy_two_pending_state();
	let expected = pending_of(&state);
	let (dir, owner, segment, whole) =
		torn_store_holding("adr0118-cold-open-justified", state, last).await;
	let store = open_as_node(&dir, &owner)
		.await
		.expect("the open of the valid policy-2 snapshot");
	assert_eq!(
		std::fs::metadata(&segment).unwrap().len(),
		whole.len() as u64,
		"the torn tail is truncated as usual"
	);
	let recovered = store
		.recovered_state_machine()
		.await
		.unwrap()
		.state_snapshot()
		.await;
	assert_eq!(
		pending_of(&recovered),
		expected,
		"the pending policy-2 target"
	);
	assert_eq!(recovered.last_applied_log_id.map(|id| id.index), Some(last));
}

/// Wendelon stage (c), the premature-settlement control through replay: a
/// committed `TableDropSettlement` in the recovered suffix of a ParentDecisions
/// store replays as the same refusal. The open succeeds, the applied index
/// passes the entry, and the tombstone, reclaim debt and descriptor are kept.
#[test]
fn a_committed_parent_decisions_settlement_replays_as_a_refusal() {
	let mut state = fixture::base(3);
	fixture::grant_all(&mut state);
	fixture::finalize_drop_of(&mut state, fixture::TABLE);
	let base_index = fixture::peek_index() - 1;
	state.last_applied_log_id = Some(LogId::new(
		openraft::CommittedLeaderId::new(1, 1),
		base_index,
	));
	let debt = state
		.table_drop_reclaim_debts
		.get(fixture::TENANT, fixture::TABLE)
		.cloned()
		.expect("premise: reclaim debt");
	let entries = vec![normal_entry(
		base_index + 1,
		WendelonRequest::TableDropSettlement(crate::consensus::TableDropSettlementRequest {
			operation_id: debt.operation_id,
			finalize_log_index: debt.finalize_log_index,
			ownership_epoch: debt.ownership_epoch,
			attested_participants: debt.participants.clone(),
		}),
	)];
	let last = entries[0].log_id;
	let recovered = recover_sm_inner(
		DecisionMode::ParentDecisions,
		Some(loaded_snapshot(
			state,
			Some(LogId::new(
				openraft::CommittedLeaderId::new(1, 1),
				base_index,
			)),
		)),
		entries,
		Some(RecoveryCheckpoint {
			commit_index: last.index,
			e_safe_index: last.index,
		}),
		false,
	)
	.expect("a refused settlement still opens")
	.state;
	// The replay refused it as apply does; its response is not kept, so the
	// state is the evidence.
	assert_eq!(recovered.last_applied_log_id, Some(last));
	assert!(
		recovered
			.table_drop_tombstones
			.get(fixture::TENANT, fixture::TABLE)
			.is_some()
	);
	assert!(
		recovered
			.table_drop_reclaim_debts
			.get(fixture::TENANT, fixture::TABLE)
			.is_some()
	);
	assert!(
		recovered
			.table_schemas
			.latest_by_id(fixture::TENANT, fixture::TABLE)
			.is_some()
	);
}

/// A table Create envelope for `(tenant, id, name)` (generation 1).
fn table_create_envelope(
	tenant: crate::schema::TenantId,
	id: crate::schema::TableObjectId,
	name: &str,
) -> Vec<u8> {
	use crate::integration::compute::{
		COMPUTE_STORAGE_CONTRACT_VERSION, SchemaGeneration, SchemaIntentId, SchemaIntentOrigin,
		SchemaMutationKind, SchemaObjectKind, SchemaObjectRef, SchemaUpdateEnvelope,
		encode_schema_update_envelope_v1,
	};
	let descriptor = crate::schema::TableSchemaDescriptor::new_with_layout(
		id,
		1,
		"ns",
		name,
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
	encode_schema_update_envelope_v1(&SchemaUpdateEnvelope {
		contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
		origin: SchemaIntentOrigin::ComputeClient,
		tenant_id: tenant,
		object: SchemaObjectRef {
			kind: SchemaObjectKind::Table,
			namespace: "ns".to_owned(),
			name: name.to_owned(),
		},
		expected_generation: SchemaGeneration::ZERO,
		requested_generation: SchemaGeneration::new(1),
		intent_id: SchemaIntentId::new(*id.as_bytes()),
		mutation: SchemaMutationKind::Create,
		payload: crate::schema::encode_table_schema_descriptor_v1(&descriptor).unwrap(),
	})
	.unwrap()
}

/// Wendelon ADR 0118 D3: under ParentDecisions a dropped table's id is never
/// reused. A state holding T's permanent drop outcome with no descriptor for T (as
/// a settlement would leave it) passes the name- and id-bound Create checks; the
/// outcome itself refuses the Create, with no effect. A fresh id under the former
/// name succeeds, and the same id under another tenant is unaffected.
#[test]
fn a_create_of_a_dropped_table_id_is_refused_under_parent_decisions() {
	let dropped = crate::schema::TableObjectId::new([0x7d; 16]);
	let mut state = fixture::base(3);
	state
		.table_drop_outcomes
		.insert_validated(crate::schema::TableDropOutcome {
			operation_id: crate::integration::compute::DropOperationId {
				tenant_id: fixture::TENANT,
				table_object_id: dropped,
				schema_generation: crate::integration::compute::SchemaGeneration::new(1),
			},
			finalize_log_index: 10,
		});
	assert!(
		state
			.table_schemas
			.latest_by_id(fixture::TENANT, dropped)
			.is_none(),
		"premise: no descriptor for the dropped id"
	);
	let before = state.table_schemas.clone();
	let refused = apply_schema_update(
		&mut state,
		&table_create_envelope(fixture::TENANT, dropped, "former"),
	);
	assert_eq!(
		refused.as_ref().err().map(String::as_str),
		Some(PD_DROPPED_TABLE_ID_REUSE_REFUSED),
		"the intended refusal: {refused:?}"
	);
	assert_eq!(state.table_schemas, before, "no effect");
	apply_schema_update(
		&mut state,
		&table_create_envelope(
			fixture::TENANT,
			crate::schema::TableObjectId::new([0x7e; 16]),
			"former",
		),
	)
	.expect("a fresh id under the former name");
	let other_tenant = crate::schema::TenantId(8);
	let other = apply_schema_update(
		&mut state,
		&table_create_envelope(other_tenant, dropped, "former"),
	);
	assert_ne!(
		other.as_ref().err().map(String::as_str),
		Some(PD_DROPPED_TABLE_ID_REUSE_REFUSED),
		"another tenant's same id is not refused as a dropped id: {other:?}"
	);
}

/// Wendelon ADR 0120: one of each CTAS chain command, pinned.
/// Wendelon ADR 0121: the bind command (as a Raft request), a target, a member's
/// report and a certificate carrying a retention cut, each with every field set.
fn golden_retention_shapes() -> Vec<(&'static str, Vec<u8>)> {
	use crate::consensus::decisions::DecisionCommand;
	use crate::consensus::decisions::generations::{
		Certificate, ChunkSum, GENERATION_CHUNKS, ReportBody, RetentionCut, Target,
	};
	let retention = RetentionCut {
		window_ms: 15_000,
		stamp_ms: 0x0121_0121,
		floor_ms: 0x0121_0121 - 15_000,
	};
	let oldest = crate::schema::HybridTimestamp {
		physical_ms: 0x0121_0000,
		logical: 7,
		node_id: 2,
	};
	let target = Target {
		gid: 121,
		t: 1_210,
		predecessor_gid: 120,
		members: vec![fixture::signer(1), fixture::signer(2), fixture::signer(3)],
		chunk_bits: crate::consensus::decisions::generations::CHUNK_BITS,
		survivor_policy: crate::consensus::decisions::generations::SURVIVOR_POLICY,
		retention,
	};
	let digest = crate::consensus::decisions::generations::target_digest(&target);
	let chunks = vec![
		ChunkSum {
			count: 1,
			root: [0x21; 32],
		};
		GENERATION_CHUNKS
	];
	vec![
		(
			"bind_retention",
			postcard::to_stdvec(&WendelonRequest::Decision(DecisionCommand::BindRetention {
				window_ms: 15_000,
			}))
			.unwrap(),
		),
		(
			"propose_target_retention",
			postcard::to_stdvec(&WendelonRequest::Decision(DecisionCommand::ProposeTarget {
				target: target.clone(),
			}))
			.unwrap(),
		),
		(
			"report_retention",
			postcard::to_stdvec(&ReportBody {
				gid: 121,
				target_digest: digest,
				reporter: fixture::signer(1),
				chunks: chunks.clone(),
				oldest_visible: Some(oldest),
			})
			.unwrap(),
		),
		(
			"certificate_retention",
			postcard::to_stdvec(&Certificate {
				gid: 121,
				target_digest: digest,
				t: 1_210,
				predecessor_gid: 120,
				chunks,
				committed_at: 1_211,
				retention,
				oldest_visible: Some(oldest),
			})
			.unwrap(),
		),
	]
}

fn golden_ctas_commands() -> Vec<(&'static str, crate::consensus::decisions::DecisionCommand)> {
	use crate::consensus::decisions::DecisionCommand;
	use crate::consensus::decisions::ctas::{
		ChunkDecl, CtasAbandonCause, CtasBegin, encode_declaration,
	};
	let tenant = crate::schema::TenantId::new(120);
	let op = [0x20; 16];
	vec![
		(
			"ctas_begin",
			DecisionCommand::CtasBegin {
				begin: CtasBegin {
					tenant,
					op,
					fingerprint: [0x21; 32],
					descriptor: fixture::fixture_table(),
					declaration: encode_declaration(&[ChunkDecl {
						sequence_no: 0x4354_4153_0000_0001,
						rows: 3,
						request_digest: [0x22; 32],
					}])
					.unwrap(),
					dispatch_hash: [0x23; 32],
					stamp_ms: 120,
				},
			},
		),
		(
			"ctas_renew",
			DecisionCommand::CtasRenew {
				tenant,
				op,
				epoch: 1,
				owner: [0x24; 32],
				stamp_ms: 121,
			},
		),
		(
			"ctas_recover",
			DecisionCommand::CtasRecover {
				tenant,
				op,
				new_epoch: 2,
				stamp_ms: 122,
			},
		),
		(
			"ctas_publish",
			DecisionCommand::CtasPublish {
				tenant,
				op,
				epoch: 1,
				owner: [0x24; 32],
				stamp_ms: 123,
			},
		),
		(
			"ctas_conclude",
			DecisionCommand::CtasConclude {
				tenant,
				op,
				epoch: 2,
				stamp_ms: 124,
			},
		),
		(
			"ctas_abandon",
			DecisionCommand::CtasAbandon {
				tenant,
				op,
				epoch: 1,
				owner: [0x24; 32],
				cause: CtasAbandonCause::SourceBytesLost,
				stamp_ms: 125,
			},
		),
		("ctas_prune", DecisionCommand::CtasPrune { tenant, op }),
	]
}
