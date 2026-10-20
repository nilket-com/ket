//! Matched profile03 evidence is historical; future comparisons require the same root format.
//! A2/A3 small-state synthetic replay; never installed or large-capacity evidence.
use super::*;
use crate::storage::mutation_visibility::root2::origin::root_and_origin2;
use crate::storage::mutation_visibility::root2::state::UnverifiedRoot2;
use crate::storage::mutation_visibility::tests::g2::Phase;
use crate::test_support::g2::{case, emit};
use serde_json::json;
struct PilotSources(LogId<u32>);
impl AcquisitionSources2 for PilotSources {
	fn applied(
		&self,
	) -> Result<
		Option<LogId<u32>>,
		crate::storage::mutation_visibility::root2::acquisition::SourceClosed2,
	> {
		Ok(Some(self.0))
	}
	fn retention(&self) -> RetentionStatus {
		RetentionStatus {
			history: HistoryCheck::Complete {
				stored_last: self.0.index,
			},
			..full()
		}
	}
}
fn expected(
	entries: &[E],
	dep: Deployment2,
	pin: &PinnedBoundTuple2,
) -> (Vec<u8>, Vec<serde_json::Value>) {
	let mut state = empty_authority2();
	let source = CommittedSource2::synthetic();
	let mut outcomes = Vec::new();
	for e in entries {
		let ce = committed_entry2(&source, e).unwrap().into_committed();
		let (pos, fp, class) = ce.entry_fields();
		state = if matches!(
			class,
			EntryClass2::Bootstrap(_) | EntryClass2::OtherRoot2(_)
		) {
			let command =
				crate::storage::mutation_visibility::root2::committed::fixture::command(pos, fp)
					.unwrap();
			let trusted = TrustedEntry2::bind(&ce, &command, dep.generation, dep.cluster).unwrap();
			let (next, outcome) =
				advance_verified2(&state, pin, &ce, &trusted, &command, None).unwrap();
			let fact = match outcome {
				ApplyResult2::Covered => json!({"kind":"Covered"}),
				ApplyResult2::Registered(first) => json!({"kind":"Registered",
					"reference":{"term":first.term,"node":first.node,"index":first.index}}),
				ApplyResult2::Denied { reason, .. } => json!({"kind":"Denied","reason":reason}),
				ApplyResult2::Permission { outcome, .. } => {
					json!({"kind":"Permission","outcome":outcome})
				}
				ApplyResult2::Parent { new_effect, .. } => {
					json!({"kind":"Parent","new_effect":new_effect})
				}
				ApplyResult2::RejectedEvidence(reason) => {
					json!({"kind":"RejectedEvidence","reason":reason})
				}
				ApplyResult2::PendingEvidence(reason) => {
					json!({"kind":"PendingEvidence","reason":reason})
				}
				other => panic!("unexpected pilot command outcome: {other:?}"),
			};
			outcomes.push(json!({"index":e.log_id.index,"result":fact}));
			next
		} else if matches!(origin_view2(&state), OriginView2::Root) {
			advance_cover2(&state, &ce).unwrap().0
		} else {
			advance_pre_root2(&state, &ce).unwrap().0
		};
	}
	(encode_authority2(&state).unwrap(), outcomes)
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "G2 A2/A3: isolated reviewed synthetic runner only"]
async fn g2_small_state_replay() {
	let c = case();
	let assay = c["assay"].as_str().unwrap();
	assert!(["A2", "A3"].contains(&assay));
	let n = c["n"].as_u64().unwrap();
	assert!([7, 256, 4096].contains(&n));
	let batch = c["max_entries"].as_u64().unwrap();
	assert!([1, 16, 256].contains(&batch));
	let (pin, dep, mut entries, mut fixtures) = context();
	let recipe = c["fixture"].as_str().unwrap();
	assert!(["bootstrap", "admission"].contains(&recipe));
	if recipe == "admission" {
		let v: serde_json::Value =
			serde_json::from_str(include_str!("../../tests/statement_vectors.json")).unwrap();
		let a = Command2::AdmissionPermission(
			unhex(v["p3_grant"]["bytes"].as_str().unwrap())
				.try_into()
				.unwrap(),
		);
		let kat: serde_json::Value =
			serde_json::from_str(include_str!("../../auth/tests/vectors.json")).unwrap();
		let claim = ClaimProjection2::decode(&unhex(kat["claim_c2"].as_str().unwrap())).unwrap();
		let holder =
			HolderAttestation2::decode(&unhex(kat["holder_signed"].as_str().unwrap())).unwrap();
		let f = Command2::Finalize {
			identity: claim.manifest.identity.clone(),
			digest: claim.digest().unwrap(),
			reservation: Log2 {
				term: 1,
				node: 1,
				index: 5,
			},
			attestation: holder,
		};
		for cmd in [a, Command2::Reserve(claim), f] {
			let e = entry(entries.len() as u64);
			fixtures.push(fixture(&e, cmd, dep));
			entries.push(e);
		}
	}
	while entries.len() < n as usize {
		let mut e = entry(entries.len() as u64);
		e.payload = EntryPayload::Normal(WendelonRequest::SchemaUpdate(vec![0xa5; 64]));
		entries.push(e);
	}
	let lookup_count = fixtures.len();
	let _scope = Root2FixtureScope::install(fixtures);
	let (want, outcomes) = expected(&entries, dep, &pin);
	let sources = PilotSources(entries.last().unwrap().log_id);
	let mut reader = store(entries).await;
	let (_scratch, mut marks) = completion::marks();
	assert_eq!(marks.durable(), Some(Baseline2::Provisioned));
	let caps = AcquisitionCaps2 {
		max_entries: batch,
		max_batch_bytes: 1 << 20,
		max_total_bytes: 1 << 27,
	};
	let phase = if assay == "A2" {
		Some(Phase::begin("run"))
	} else {
		None
	};
	let replayed = run(&mut reader, &sources, caps, dep, &pin, &mut marks)
		.await
		.unwrap();
	if let Some(p) = phase {
		emit(p.end());
	}
	assert_eq!(replayed.report().entries(), n);
	assert_eq!(marks.durable(), Some(Baseline2::Mark(replayed.mark())));
	assert_eq!(encode_authority2(replayed.state()).unwrap(), want);
	let (root, _) = root_and_origin2(replayed.state()).unwrap();
	let bytes = root.encode().unwrap();
	assert_eq!(&bytes[..8], b"VISAUT04");
	let u = root.usage().unwrap();
	emit(
		json!({"event":"replay","case":c,"fixture_commands":lookup_count,"outcomes":outcomes,
		"encoded_bytes":bytes.len(),"encoded_digest":crate::storage::mutation_visibility::root2::codec::hash(b"",&bytes),"parents":u.parents,"held":u.held,"history":u.history,"registry":u.registry,
		"lineages":root.state.lineages.len(),"cuts":root.state.cuts.len(),"permissions":root.state.permissions.len(),
		"scope":"source/prefix replay cost with small authority state; synthetic observations"}),
	);
	if assay == "A3" {
		match c["op"].as_str().unwrap() {
			"encode" => {
				let p = Phase::begin("encode");
				let output = root.encode().unwrap();
				let metric = p.end();
				assert_eq!(output, bytes);
				emit(metric);
				std::hint::black_box(output);
			}
			"decode" => {
				let p = Phase::begin("decode");
				let output = UnverifiedRoot2::decode(&bytes).unwrap();
				let metric = p.end();
				assert_eq!(output.encode().unwrap(), bytes);
				emit(metric);
				std::hint::black_box(output);
			}
			_ => panic!("undeclared snapshot operation"),
		}
	}
}
