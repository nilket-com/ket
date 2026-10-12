//! Current-thread composition fixtures, with exact-entry scoped classification. No carrier proof.
use super::*;
use crate::consensus::retention::{ControlRetention, HistoryCheck, RetentionRefusals};
use crate::consensus::{RetentionStatus, WendelonLogStore, WendelonRequest};
use crate::storage::mutation_visibility::root2::admin::manifest::{
	AdminManifest2, AnyAdminManifest2, admin_manifest_digest2,
};
use crate::storage::mutation_visibility::root2::admin::record::BootstrapRecord2;
use crate::storage::mutation_visibility::root2::committed::fixture::Root2FixtureScope;
use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
use crate::storage::mutation_visibility::root2::origin::encode_authority2;
use crate::storage::mutation_visibility::root2::provisioning::inventory::{
	LogState2, RootEntry2, RootIdentity2, RootState2,
};
use crate::storage::mutation_visibility::root2::provisioning::replica_anchor::{
	ReplicaInputs2, validate_replica_tuple2,
};
use crate::storage::mutation_visibility::root2::provisioning::snapshot::encode_snapshot2;
use crate::storage::mutation_visibility::root2::recovery::{RecoveryMark2, mark_of2};
use crate::storage::mutation_visibility::root2::types::{
	ClaimProjection2, Digest2, HolderAttestation2,
};
use openraft::storage::RaftLogStorageExt;
use openraft::{BasicNode, CommittedLeaderId, EntryPayload, LogId, Membership};
use std::collections::BTreeMap;

type E = Entry<WendelonTypeConfig>;
type Fixture = (EntryPosition2, Digest2, EntryClass2, Command2);
fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn vector(name: &str) -> Vec<u8> {
	let v: serde_json::Value =
		serde_json::from_str(include_str!("../tests/statement_vectors.json")).unwrap();
	unhex(v[name].as_str().unwrap())
}
fn full() -> RetentionStatus {
	RetentionStatus {
		configured: ControlRetention::Full,
		history: HistoryCheck::Complete { stored_last: 9 },
		violation: None,
		refusals: RetentionRefusals::default(),
	}
}
struct Sources(LogId<u32>);
impl AcquisitionSources2 for Sources {
	fn applied(
		&self,
	) -> Result<
		Option<LogId<u32>>,
		crate::storage::mutation_visibility::root2::acquisition::SourceClosed2,
	> {
		Ok(Some(self.0))
	}
	fn retention(&self) -> RetentionStatus {
		full()
	}
}
fn caps() -> AcquisitionCaps2 {
	AcquisitionCaps2 {
		max_entries: 2,
		max_batch_bytes: 1 << 20,
		max_total_bytes: 1 << 30,
	}
}
fn entry(index: u64) -> E {
	if index == 0 {
		Entry {
			log_id: LogId::default(),
			payload: EntryPayload::Membership(Membership::from(BTreeMap::from([(
				1,
				BasicNode { addr: "n1".into() },
			)]))),
		}
	} else {
		Entry {
			log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
			payload: EntryPayload::Normal(WendelonRequest::SchemaUpdate(vec![index as u8])),
		}
	}
}
fn fields(e: &E) -> (EntryPosition2, Digest2) {
	let ce = committed_entry2(&CommittedSource2::synthetic(), e).unwrap();
	let (p, fp, _) = ce.entry_fields();
	(p, fp)
}
fn fixture(e: &E, cmd: Command2, dep: Deployment2) -> Fixture {
	let (p, fp) = fields(e);
	let d = cmd.digest(dep.generation, dep.cluster).unwrap();
	let class = if matches!(cmd, Command2::Bootstrap { .. }) {
		EntryClass2::Bootstrap(d)
	} else {
		EntryClass2::OtherRoot2(d)
	};
	(p, fp, class, cmd)
}
fn context() -> (PinnedBoundTuple2, Deployment2, Vec<E>, Vec<Fixture>) {
	let bytes = vector("manifest_prime");
	let m = AdminManifest2::decode(&bytes).unwrap();
	let pin = admin_manifest_digest2(bytes.as_slice().try_into().unwrap());
	let dep = Deployment2 {
		generation: m.generation,
		cluster: m.cluster,
	};
	let entries = (0..=3).map(entry).collect::<Vec<_>>();
	let cmd = Command2::Bootstrap {
		epoch: m.initial_epoch,
		control: pin,
		length: bytes.len() as u64,
	};
	let mut record = BootstrapRecord2::decode(&vector("record")).unwrap();
	let (p, fp) = fields(&entries[2]);
	record.identity.bootstrap = Log2 {
		term: p.term,
		node: p.node,
		index: p.index,
	};
	record.identity.bootstrap_fingerprint = fp;
	record.identity.genesis_fingerprint = fields(&entries[0]).1;
	record.identity.bootstrap_command = cmd.digest(dep.generation, dep.cluster).unwrap();
	let seq = record.inventory_seq;
	let snapshot = encode_snapshot2(&LogState2 {
		roots: vec![RootEntry2 {
			identity: RootIdentity2 {
				generation: m.generation,
				cluster: m.cluster,
				pin,
				admin_key: m.admin_key,
			},
			manifest: AnyAdminManifest2::One(m),
			state: RootState2::Bound,
			last_seq: seq,
			record: Some(record),
		}],
		last_seq: seq,
	})
	.unwrap();
	let ip = crate::storage::mutation_visibility::root2::codec::hash(b"", &snapshot);
	let tuple = validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(&bytes),
		manifest_pin: Some(&pin),
		snapshot: Some(&snapshot),
		inventory_pin: Some(&ip),
	})
	.unwrap();
	let fixtures = vec![fixture(&entries[2], cmd, dep)];
	(tuple, dep, entries, fixtures)
}
async fn store(entries: Vec<E>) -> WendelonLogStore {
	let mut s = WendelonLogStore::in_memory();
	s.blocking_append(entries).await.unwrap();
	s
}
// Independent per-API oracle: no driver calls; the exact fixture classes bind the real entries.
fn oracle(
	entries: &[E],
	dep: Deployment2,
	pin: &PinnedBoundTuple2,
) -> Vec<MutationAuthorityState2> {
	let mut out = Vec::new();
	let empty = empty_authority2();
	let source = CommittedSource2::synthetic();
	for e in entries {
		let old = out.last().unwrap_or(&empty);
		let ce = committed_entry2(&source, e).unwrap();
		let (p, fp, class) = ce.entry_fields();
		let next = match (origin_view2(old), class) {
			(_, EntryClass2::Bootstrap(_) | EntryClass2::OtherRoot2(_))
				if !matches!(origin_view2(old), OriginView2::PreRoot { .. })
					|| matches!(class, EntryClass2::Bootstrap(_)) =>
			{
				let cmd =
					crate::storage::mutation_visibility::root2::committed::fixture::command(p, fp)
						.unwrap();
				let trusted = TrustedEntry2::bind(&ce, &cmd, dep.generation, dep.cluster).unwrap();
				advance_verified2(old, pin, &ce, &trusted, &cmd).unwrap().0
			}
			(OriginView2::Root, _) => advance_cover2(old, &ce).unwrap().0,
			_ => advance_pre_root2(old, &ce).unwrap().0,
		};
		out.push(next);
	}
	out
}
#[tokio::test(flavor = "current_thread")]
async fn full_composition_matches_per_api_and_observes_each_entry() {
	let (pin, dep, mut entries, mut fixtures) = context();
	let v: serde_json::Value =
		serde_json::from_str(include_str!("../tests/statement_vectors.json")).unwrap();
	let adm = Command2::AdmissionPermission(
		unhex(v["grant"]["bytes"].as_str().unwrap())
			.try_into()
			.unwrap(),
	);
	let kat: serde_json::Value =
		serde_json::from_str(include_str!("../auth/tests/vectors.json")).unwrap();
	let claim = ClaimProjection2::decode(&unhex(kat["claim_c2"].as_str().unwrap())).unwrap();
	let holder =
		HolderAttestation2::decode(&unhex(kat["holder_signed"].as_str().unwrap())).unwrap();
	let finalize = Command2::Finalize {
		identity: claim.manifest.identity.clone(),
		digest: claim.digest().unwrap(),
		reservation: Log2 {
			term: 1,
			node: 1,
			index: 5,
		},
		attestation: holder,
	};
	for cmd in [adm, Command2::Reserve(claim), finalize] {
		let e = entry(entries.len() as u64);
		fixtures.push(fixture(&e, cmd, dep));
		entries.push(e);
	}
	let _scope = Root2FixtureScope::install(fixtures);
	let expected = oracle(&entries, dep, &pin);
	let sources = Sources(entries.last().unwrap().log_id);
	let mut reader = store(entries.clone()).await;
	let (acc, report) = ReplayDriver2 {
		deployment: dep,
		baseline: Baseline2::Provisioned,
		pin: &pin,
	}
	.compose(&mut reader, &sources, caps())
	.await
	.unwrap();
	assert_eq!(
		encode_authority2(&acc.state).unwrap(),
		encode_authority2(expected.last().unwrap()).unwrap()
	);
	assert_eq!(acc.traversed, 7);
	assert_eq!(report.entries(), 7);
	assert_eq!(report.terminal_applied(), sources.0);
	assert_eq!(
		report.terminal_retention().configured,
		ControlRetention::Full
	);
	assert!(matches!(acc.progress, Progress2::Ready(_)));
	let (_d, mut marks) = completion::marks();
	let mut reader = store(entries).await;
	let replayed = run(&mut reader, &sources, caps(), dep, &pin, &mut marks)
		.await
		.unwrap();
	assert_eq!(
		encode_authority2(replayed.state()).unwrap(),
		encode_authority2(expected.last().unwrap()).unwrap()
	);
	assert_eq!(replayed.report().entries(), 7);
	assert_eq!(marks.durable(), Some(Baseline2::Mark(replayed.mark())));
}
#[tokio::test(flavor = "current_thread")]
async fn saved_mark_at_cut_still_checks_command_and_full_state() {
	let (pin, dep, entries, fixtures) = context();
	let _scope = Root2FixtureScope::install(fixtures);
	let states = oracle(&entries, dep, &pin);
	let saved = mark_of2(states.last().unwrap()).unwrap();
	for baseline in [
		saved,
		RecoveryMark2 {
			command: [0x9a; 32],
			..saved
		},
		RecoveryMark2 {
			state: [0x9b; 32],
			..saved
		},
	] {
		let mut reader = store(entries.clone()).await;
		let result = ReplayDriver2 {
			deployment: dep,
			baseline: Baseline2::Mark(baseline),
			pin: &pin,
		}
		.compose(
			&mut reader,
			&Sources(entries.last().unwrap().log_id),
			caps(),
		)
		.await;
		if baseline == saved {
			assert!(result.is_ok());
		} else {
			assert!(matches!(
				result,
				Err(ReplayError2::Fault(Fault2::Observe(
					RecoveryError2::Conflict
				)))
			));
		}
	}
}
#[tokio::test(flavor = "current_thread")]
async fn target_below_saved_mark_refuses_initialization() {
	let (pin, dep, entries, fixtures) = context();
	let _scope = Root2FixtureScope::install(fixtures);
	let saved = mark_of2(oracle(&entries, dep, &pin).last().unwrap()).unwrap();
	let mut reader = store(entries.clone()).await;
	let result = ReplayDriver2 {
		deployment: dep,
		baseline: Baseline2::Mark(saved),
		pin: &pin,
	}
	.compose(&mut reader, &Sources(entries[2].log_id), caps())
	.await;
	assert!(matches!(
		result,
		Err(ReplayError2::Fault(Fault2::MonitorInit(
			RecoveryError2::TargetBelowMark
		)))
	));
}
#[tokio::test(flavor = "current_thread")]
async fn pre_root_target_refuses_and_expected_mode_keeps_old_semantics() {
	let (pin, dep, entries, _) = context();
	let mut reader = store(entries[..2].to_vec()).await;
	let driver = ReplayDriver2 {
		deployment: dep,
		baseline: Baseline2::Provisioned,
		pin: &pin,
	};
	assert!(matches!(
		driver
			.compose(&mut reader, &Sources(entries[1].log_id), caps())
			.await,
		Err(ReplayError2::Fault(Fault2::Observe(
			RecoveryError2::TargetNotRooted
		)))
	));
	let mut target = ReplayTarget2 {
		cut: Log2 {
			term: 1,
			node: 1,
			index: 1,
		},
		fingerprint: fields(&entries[1]).1,
		command: TargetCommand2::Expected([0; 32]),
	};
	let source = CommittedSource2::synthetic();
	let mut m = replay_monitor2(dep, Some(Baseline2::Provisioned), target).unwrap();
	let mut state = empty_authority2();
	for e in &entries[..2] {
		state = advance_pre_root2(&state, &committed_entry2(&source, e).unwrap())
			.unwrap()
			.0;
		assert!(matches!(
			observe2(&mut m, &state).unwrap(),
			Progress2::Replaying
		));
	}
	target.command = TargetCommand2::BoundAtCut;
	let mut m = replay_monitor2(dep, Some(Baseline2::Provisioned), target).unwrap();
	let genesis = advance_pre_root2(
		&empty_authority2(),
		&committed_entry2(&source, &entries[0]).unwrap(),
	)
	.unwrap()
	.0;
	observe2(&mut m, &genesis).unwrap();
	assert_eq!(
		observe2(&mut m, &state).err(),
		Some(RecoveryError2::TargetNotRooted)
	);
	assert_eq!(
		observe2(&mut m, &state).err(),
		Some(RecoveryError2::Latched)
	);
}
#[tokio::test(flavor = "current_thread")]
async fn unsupported_command_halts_before_later_batch_error() {
	let (pin, dep, entries, mut fixtures) = context();
	let cmd = Command2::Cut {
		expected: 1,
		next: 2,
		configuration: [4; 32],
	};
	fixtures.push(fixture(&entries[3], cmd, dep));
	let _scope = Root2FixtureScope::install(fixtures);
	let mut reader = store(entries.clone()).await;
	// No entry 5: a later range would fail coverage, but the fold at 3 fails first.
	let result = ReplayDriver2 {
		deployment: dep,
		baseline: Baseline2::Provisioned,
		pin: &pin,
	}
	.compose(&mut reader, &Sources(entry(5).log_id), caps())
	.await;
	assert!(matches!(result, Err(ReplayError2::Acquisition(_))));
	// With a real boundary at 5 but a hole at 4, the earlier unsupported fold wins.
	reader.blocking_append(vec![entry(5)]).await.unwrap();
	let result = ReplayDriver2 {
		deployment: dep,
		baseline: Baseline2::Provisioned,
		pin: &pin,
	}
	.compose(&mut reader, &Sources(entry(5).log_id), caps())
	.await;
	assert!(matches!(
		result,
		Err(ReplayError2::Fault(Fault2::Unsupported(
			UnsupportedKind2::Cut
		)))
	));
}
#[tokio::test(flavor = "current_thread")]
async fn malformed_before_root_is_recorded_and_after_root_is_terminal() {
	for before in [true, false] {
		let (pin, dep, entries, mut fixtures) = context();
		let i = if before { 1 } else { 3 };
		let (p, fp) = fields(&entries[i]);
		fixtures.push((
			p,
			fp,
			EntryClass2::MalformedRoot2,
			Command2::Cover {
				kind: 0,
				external: fp,
			},
		));
		let _scope = Root2FixtureScope::install(fixtures);
		let mut reader = store(entries.clone()).await;
		let result = ReplayDriver2 {
			deployment: dep,
			baseline: Baseline2::Provisioned,
			pin: &pin,
		}
		.compose(&mut reader, &Sources(entries[3].log_id), caps())
		.await;
		if before {
			let (acc, _) = result.unwrap();
			let expected = oracle(&entries, dep, &pin);
			assert_eq!(
				encode_authority2(&acc.state).unwrap(),
				encode_authority2(expected.last().unwrap()).unwrap()
			);
		} else {
			assert!(matches!(
				result,
				Err(ReplayError2::Fault(Fault2::Cover(
					CoverError2::MalformedAfterRoot
				)))
			));
		}
	}
}
#[tokio::test(flavor = "current_thread")]
async fn other_root_before_bootstrap_uses_existing_malformed_rule() {
	let (pin, dep, entries, mut fixtures) = context();
	fixtures.push(fixture(
		&entries[1],
		Command2::Cut {
			expected: 1,
			next: 2,
			configuration: [8; 32],
		},
		dep,
	));
	let _scope = Root2FixtureScope::install(fixtures);
	let mut reader = store(entries.clone()).await;
	let (acc, _) = ReplayDriver2 {
		deployment: dep,
		baseline: Baseline2::Provisioned,
		pin: &pin,
	}
	.compose(&mut reader, &Sources(entries[3].log_id), caps())
	.await
	.unwrap();
	let source = CommittedSource2::synthetic();
	let genesis = advance_pre_root2(
		&empty_authority2(),
		&committed_entry2(&source, &entries[0]).unwrap(),
	)
	.unwrap()
	.0;
	assert!(matches!(
		advance_pre_root2(&genesis, &committed_entry2(&source, &entries[1]).unwrap())
			.unwrap()
			.1,
		PreRootStep2::Malformed
	));
	assert_eq!(
		encode_authority2(&acc.state).unwrap(),
		encode_authority2(oracle(&entries, dep, &pin).last().unwrap()).unwrap()
	);
}
#[tokio::test(flavor = "current_thread")]
async fn fixture_exact_identity_and_scope_cleanup_on_cancellation() {
	let (_, _, entries, fixtures) = context();
	let p = fields(&entries[2]).0;
	let fp = fields(&entries[2]).1;
	{
		let mut future = Box::pin(async move {
			let _scope = Root2FixtureScope::install(fixtures);
			std::future::pending::<()>().await;
		});
		tokio::select! {biased; _=&mut future=>unreachable!(), _=tokio::task::yield_now()=>{}}
		assert!(
			crate::storage::mutation_visibility::root2::committed::fixture::command(p, fp)
				.is_some()
		);
		assert!(
			crate::storage::mutation_visibility::root2::committed::fixture::command(
				EntryPosition2 { term: 2, ..p },
				fp
			)
			.is_none()
		);
		assert!(
			crate::storage::mutation_visibility::root2::committed::fixture::command(p, [0xff; 32])
				.is_none()
		);
	}
	assert!(
		crate::storage::mutation_visibility::root2::committed::fixture::command(p, fp).is_none()
	);
	// A replacement scope succeeds after cancellation; nesting still panics without losing it.
	let _scope = Root2FixtureScope::install(vec![]);
	assert!(std::panic::catch_unwind(|| Root2FixtureScope::install(vec![])).is_err());
}
#[test]
fn trusted_binding_refuses_wrong_class_digest_and_nonpositive_positions() {
	let (_, dep, entries, _) = context();
	let cmd = Command2::Cut {
		expected: 1,
		next: 2,
		configuration: [9; 32],
	};
	let (p, fp) = fields(&entries[2]);
	let d = cmd.digest(dep.generation, dep.cluster).unwrap();
	for (pos, class, want) in [
		(p, EntryClass2::NonRoot2, BindError2::NotRoot2Command),
		(
			p,
			EntryClass2::OtherRoot2([0; 32]),
			BindError2::DigestMismatch,
		),
		(
			EntryPosition2 { term: 0, ..p },
			EntryClass2::OtherRoot2(d),
			BindError2::Position,
		),
		(
			EntryPosition2 { node: 0, ..p },
			EntryClass2::OtherRoot2(d),
			BindError2::Position,
		),
		(
			EntryPosition2 { index: 0, ..p },
			EntryClass2::OtherRoot2(d),
			BindError2::Position,
		),
	] {
		let ce = CommittedEntry2::fixture(pos, fp, class);
		assert_eq!(
			TrustedEntry2::bind(&ce, &cmd, dep.generation, dep.cluster).err(),
			Some(want)
		);
	}
}

struct DriftReader {
	inner: WendelonLogStore,
	reads: usize,
}
impl RaftLogReader<WendelonTypeConfig> for DriftReader {
	async fn try_get_log_entries<
		RB: std::ops::RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend,
	>(
		&mut self,
		range: RB,
	) -> Result<Vec<E>, openraft::StorageError<u32>> {
		self.reads += 1;
		let mut entries = self.inner.try_get_log_entries(range).await?;
		if self.reads == 2 {
			assert_eq!(entries.len(), 1);
			entries[0].payload = EntryPayload::Normal(WendelonRequest::SchemaUpdate(vec![0xff]));
		}
		Ok(entries)
	}
}
#[tokio::test(flavor = "current_thread")]
async fn immutable_first_boundary_fingerprint_refuses_final_drift() {
	let (pin, dep, entries, fixtures) = context();
	let _scope = Root2FixtureScope::install(fixtures);
	let sources = Sources(entries[3].log_id);
	let mut reader = DriftReader {
		inner: store(entries).await,
		reads: 0,
	};
	let result = ReplayDriver2 {
		deployment: dep,
		baseline: Baseline2::Provisioned,
		pin: &pin,
	}
	.compose(&mut reader, &sources, caps())
	.await;
	assert_eq!(reader.reads, 4);
	assert!(matches!(
		result,
		Err(ReplayError2::Fault(Fault2::Observe(
			RecoveryError2::Conflict
		)))
	));
}
#[tokio::test(flavor = "current_thread")]
async fn binding_fault_is_retained_and_unavailable_carrier_refuses() {
	let (pin, dep, entries, mut fixtures) = context();
	fixtures[0].2 = EntryClass2::Bootstrap([0x88; 32]);
	let _scope = Root2FixtureScope::install(fixtures);
	let mut reader = store(entries.clone()).await;
	let driver = ReplayDriver2 {
		deployment: dep,
		baseline: Baseline2::Provisioned,
		pin: &pin,
	};
	let result = driver
		.compose(&mut reader, &Sources(entries[3].log_id), caps())
		.await;
	assert!(matches!(
		result,
		Err(ReplayError2::Fault(Fault2::Bind(
			BindError2::DigestMismatch
		)))
	));
	let (p, fp) = fields(&entries[3]);
	let uncarried = CommittedEntry2::fixture(p, fp, EntryClass2::OtherRoot2([7; 32]));
	assert!(matches!(command_at(&uncarried), Err(Fault2::NoCarrier)));
}

mod completion;
