//! Real same-entry carrier path. Legacy scaffolding remains a separate oracle, never extraction.
use super::*;
use crate::consensus::{Root2Carrier1, Root2CarrierError1};
use crate::storage::mutation_visibility::root2::origin::root_and_origin2;
use crate::storage::mutation_visibility::root2::recovery::encodings_for_test;
use crate::storage::mutation_visibility::root2::types::NodeRef2;

fn carried(command: &Command2, dep: Deployment2, index: u64, sidecar: Option<&[u8]>) -> E {
	let mut e = entry(index);
	e.payload = EntryPayload::Normal(WendelonRequest::Root2Command(
		Root2Carrier1::new(
			&command.encode(dep.generation, dep.cluster).unwrap(),
			sidecar,
		)
		.unwrap(),
	));
	e
}
fn commands(dep: Deployment2) -> Vec<Command2> {
	let (_, _, _, fixtures) = context();
	let kat: serde_json::Value =
		serde_json::from_str(include_str!("../../auth/tests/vectors.json")).unwrap();
	let claim = ClaimProjection2::decode(&unhex(kat["claim_c2"].as_str().unwrap())).unwrap();
	let holder =
		HolderAttestation2::decode(&unhex(kat["holder_signed"].as_str().unwrap())).unwrap();
	let original = claim.original.clone();
	// grant is an object in the statement vectors, not a top-level hex string.
	let v: serde_json::Value =
		serde_json::from_str(include_str!("../../tests/statement_vectors.json")).unwrap();
	let mut out = vec![
		fixtures[0].3.clone(),
		Command2::AdmissionPermission(
			unhex(v["grant"]["bytes"].as_str().unwrap())
				.try_into()
				.unwrap(),
		),
	];
	out.extend([
		Command2::Reserve(claim.clone()),
		Command2::Finalize {
			identity: claim.manifest.identity.clone(),
			digest: claim.digest().unwrap(),
			reservation: Log2 {
				term: 1,
				node: 1,
				index: 5,
			},
			attestation: holder,
		},
		Command2::Cover {
			kind: 0,
			external: [3; 32],
		},
		Command2::EnrollSelector(claim.manifest.binding()),
		Command2::EnrollAuthority {
			authority: claim.authority,
			node: NodeRef2 {
				node: 1,
				incarnation: [1; 16],
			},
			key: [1; 32],
		},
		Command2::RevokeAuthority(claim.authority),
		Command2::Grant {
			lineage: original.lineage,
			incarnation: original.incarnation,
			lease: original.lease.clone(),
			predecessor: None,
		},
		Command2::Release {
			lineage: original.lineage,
			latest: original.reference().unwrap(),
		},
		Command2::Cut {
			expected: 1,
			next: 2,
			configuration: [4; 32],
		},
		Command2::Resolve {
			identity: claim.manifest.identity,
			digest: [5; 32],
			reservation: Log2 {
				term: 1,
				node: 1,
				index: 5,
			},
			cause: 1,
			anchor: Log2 {
				term: 1,
				node: 1,
				index: 4,
			},
		},
		Command2::EnrollLeaseIssuer {
			node: 1,
			key: [0; 32],
		},
		Command2::RevokeLeaseIssuer { node: 1 },
	]);
	for cmd in &out {
		Command2::decode(&cmd.encode(dep.generation, dep.cluster).unwrap()).unwrap();
	}
	out
}
fn snapshot(acc: &Provisional2) -> (Vec<u8>, u64, Progress2, u32) {
	(
		encode_authority2(&acc.state).unwrap(),
		acc.traversed,
		acc.progress,
		encodings_for_test(&acc.monitor),
	)
}
fn real_context() -> (PinnedBoundTuple2, Deployment2, Vec<E>, Vec<Fixture>) {
	let (old, dep, mut entries, mut fixtures) = context();
	let boot = fixtures[0].3.clone();
	entries[2] = carried(&boot, dep, 2, None);
	fixtures[0] = fixture(&entries[2], boot.clone(), dep);
	let bytes = vector("manifest_prime");
	let m = AdminManifest2::decode(&bytes).unwrap();
	let pin = admin_manifest_digest2(bytes.as_slice().try_into().unwrap());
	let mut record = BootstrapRecord2::decode(&vector("record")).unwrap();
	record.identity.bootstrap = Log2 {
		term: 1,
		node: 1,
		index: 2,
	};
	record.identity.bootstrap_fingerprint = fields(&entries[2]).1;
	record.identity.genesis_fingerprint = fields(&entries[0]).1;
	record.identity.bootstrap_command = boot.digest(dep.generation, dep.cluster).unwrap();
	let seq = record.inventory_seq;
	let data = encode_snapshot2(&LogState2 {
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
	let ip = crate::storage::mutation_visibility::root2::codec::hash(b"", &data);
	let tuple = validate_replica_tuple2(&ReplicaInputs2 {
		manifest: Some(&bytes),
		manifest_pin: Some(&pin),
		snapshot: Some(&data),
		inventory_pin: Some(&ip),
	})
	.unwrap();
	assert_ne!(
		crate::storage::mutation_visibility::root2::provisioning::replica_anchor::tuple_record2(
			&old
		)
		.identity
		.bootstrap_fingerprint,
		fields(&entries[2]).1
	);
	(tuple, dep, entries, fixtures)
}
#[tokio::test(flavor = "current_thread")]
async fn carrier_wal_replay_matches_same_identity_fixture_oracle_and_confirms_mark() {
	let (pin, dep, mut entries, mut fixtures) = real_context();
	let all = commands(dep);
	for command in [all[1].clone(), all[2].clone(), all[3].clone()] {
		let e = carried(&command, dep, entries.len() as u64, None);
		fixtures.push(fixture(&e, command, dep));
		entries.push(e);
	}
	let expected = {
		let _scope = Root2FixtureScope::install(fixtures.clone());
		oracle(&entries, dep, &pin)
	};
	// Independent streamed fingerprint agrees with the ordinary postcard request bytes.
	for e in &entries {
		if let EntryPayload::Normal(request) = &e.payload {
			let payload = postcard::to_stdvec(request).unwrap();
			let mut preimage = b"VISROOT2-ENTRY\0".to_vec();
			preimage.extend(1u16.to_le_bytes());
			preimage.extend(e.log_id.leader_id.term.to_le_bytes());
			preimage.extend(e.log_id.leader_id.node_id.to_le_bytes());
			preimage.extend(e.log_id.index.to_le_bytes());
			preimage.push(1);
			preimage.extend((payload.len() as u64).to_le_bytes());
			preimage.extend(payload);
			let (_, fp) = fields(e);
			let expected = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &preimage);
			assert_eq!(fp.as_slice(), expected.as_ref());
		}
	}
	let polluted = fixtures
		.iter()
		.map(|(p, fp, _, _)| {
			(
				*p,
				*fp,
				EntryClass2::OtherRoot2([8; 32]),
				Command2::Cover {
					kind: 0,
					external: [9; 32],
				},
			)
		})
		.collect();
	let _scope = Root2FixtureScope::install(polluted);
	let directory = crate::test_support::TestDir::new("v-s2-real-carrier-wal");
	let open = || {
		WendelonLogStore::open_with_file_ops(
			directory.as_ref().join("wal"),
			directory.as_ref().join("vote"),
			directory.as_ref().join("snapshot"),
			std::sync::Arc::new(crate::storage::file_ops::BlockingFileOps),
		)
	};
	let mut writer = open().await.unwrap();
	writer.blocking_append(entries.clone()).await.unwrap();
	drop(writer);
	let mut reader = open().await.unwrap();
	let recovered = reader.try_get_log_entries(0..=6).await.unwrap();
	assert_eq!(
		postcard::to_stdvec(&recovered).unwrap(),
		postcard::to_stdvec(&entries).unwrap()
	);
	let (_d, mut marks) = completion::marks();
	let replayed = run(
		&mut reader,
		&Sources(entries.last().unwrap().log_id),
		caps(),
		dep,
		&pin,
		&mut marks,
	)
	.await
	.unwrap();
	assert_eq!(
		encode_authority2(replayed.state()).unwrap(),
		encode_authority2(expected.last().unwrap()).unwrap()
	);
	assert_eq!(replayed.report().entries(), 7);
	assert_eq!(replayed.mark(), mark_of2(expected.last().unwrap()).unwrap());
	assert_eq!(marks.durable(), Some(Baseline2::Mark(replayed.mark())));
}
#[test]
fn carrier_all_fourteen_kinds_sidecars_and_origins_obey_selected_first_errors() {
	let (_, dep, _, _) = context();
	let all = commands(dep);
	for view in 0..3 {
		for (i, command) in all.iter().enumerate() {
			for side in [None, Some(&[7u8; 296][..])] {
				let (pin, dep, entries, mut fixtures) = real_context();
				let e = carried(command, dep, if view == 1 { 2 } else { 3 }, side);
				let (p, fp) = fields(&e);
				fixtures.retain(|f| (f.0, f.1) != (p, fp));
				fixtures.push((
					p,
					fp,
					EntryClass2::OtherRoot2([0; 32]),
					Command2::Cover {
						kind: 0,
						external: [0; 32],
					},
				));
				let _scope = Root2FixtureScope::install(fixtures);
				let driver = ReplayDriver2 {
					deployment: dep,
					baseline: Baseline2::Provisioned,
					pin: &pin,
				};
				let source = CommittedSource2::synthetic();
				let target = if view == 2 { &e } else { &entries[3] };
				let mut acc = driver.initialize(&source, target).unwrap();
				let count = match view {
					0 => 0,
					1 => 2,
					_ => 3,
				};
				for entry in &entries[..count] {
					driver.dispatch(&mut acc, &source, entry).unwrap();
				}
				let before = snapshot(&acc);
				let result = driver.dispatch(&mut acc, &source, &e);
				if i >= 5 {
					let expected = if view != 2 {
						VerifyError2::StateKind
					} else if side.is_none() {
						VerifyError2::MissingSidecar
					} else {
						crate::storage::mutation_visibility::root2::tests::governance_verification::support::malformed_sidecar_error()
					};
					assert!(matches!(result, Err(Fault2::Verify(e)) if e == expected));
				} else if i == 4 {
					assert!(matches!(
						result,
						Err(Fault2::Verify(VerifyError2::NotApplicable))
					));
				} else if view == 0 || (view == 1 && i != 0) || (view == 2 && i == 0) {
					assert!(matches!(
						result,
						Err(Fault2::Verify(VerifyError2::StateKind))
					));
				} else if side.is_some() {
					assert!(matches!(
						result,
						Err(Fault2::Verify(VerifyError2::ExtraSidecar))
					));
				} else {
					assert!(
						matches!(result, Ok(())),
						"supported None row: view={view} kind={i}"
					);
					let (root, _) = root_and_origin2(&acc.state).unwrap();
					assert_eq!(
						root.state.covered,
						Log2 {
							term: p.term,
							node: p.node,
							index: p.index
						}
					);
					assert_eq!(root.state.fingerprint, fp);
					assert_eq!(
						root.state.command_digest,
						command.digest(dep.generation, dep.cluster).unwrap()
					);
					assert!(
						acc.progress
							== if view == 2 {
								Progress2::Ready(mark_of2(&acc.state).unwrap())
							} else {
								Progress2::Replaying
							}
					);
					continue;
				}
				assert_eq!(snapshot(&acc), before);
			}
		}
	}
}
#[test]
fn carrier_context_precedes_bind_and_zero_coordinates_in_every_origin() {
	let (_, dep, _, _) = context();
	let all = commands(dep);
	for view in 0..3 {
		for i in [0usize, 2, 4, 12] {
			for coordinate in 0..4 {
				for wrong in 0..3 {
					let (pin, dep, entries, fixtures) = context();
					let _scope = Root2FixtureScope::install(fixtures);
					let driver = ReplayDriver2 {
						deployment: dep,
						baseline: Baseline2::Provisioned,
						pin: &pin,
					};
					let source = CommittedSource2::synthetic();
					let mut acc = driver.initialize(&source, &entries[3]).unwrap();
					let count = match view {
						0 => 0,
						1 => 2,
						_ => 3,
					};
					for e in &entries[..count] {
						driver.dispatch(&mut acc, &source, e).unwrap();
					}
					let mut ctx = dep;
					if wrong == 1 {
						ctx.generation[0] ^= 1;
					}
					if wrong == 2 {
						ctx.cluster[0] ^= 1;
					}
					let mut e = carried(&all[i], ctx, 3, Some(&[1; 296]));
					let (term, node, index) = match coordinate {
						0 => (0, 1, 3),
						1 => (1, 0, 3),
						2 => (1, 1, 0),
						_ => (0, 0, 0),
					};
					e.log_id = LogId::new(CommittedLeaderId::new(term, node), index);
					let before = snapshot(&acc);
					let result = driver.dispatch(&mut acc, &source, &e);
					if wrong != 0 {
						assert!(matches!(
							result,
							Err(Fault2::Verify(VerifyError2::EnvelopeContext))
						));
					} else {
						assert!(matches!(result, Err(Fault2::Bind(BindError2::Position))));
					}
					assert_eq!(snapshot(&acc), before);
				}
			}
		}
	}
	// Positive positions: separately wrong generation and cluster at PreRoot Bootstrap and rooted Reserve.
	for (view, i) in [(1, 0), (2, 2)] {
		for wrong in 1..3 {
			let (pin, dep, entries, fixtures) = context();
			let _scope = Root2FixtureScope::install(fixtures);
			let driver = ReplayDriver2 {
				deployment: dep,
				baseline: Baseline2::Provisioned,
				pin: &pin,
			};
			let source = CommittedSource2::synthetic();
			let mut acc = driver.initialize(&source, &entries[3]).unwrap();
			for e in &entries[..view + 1] {
				driver.dispatch(&mut acc, &source, e).unwrap();
			}
			let mut ctx = dep;
			if wrong == 1 {
				ctx.generation[0] ^= 1
			} else {
				ctx.cluster[0] ^= 1
			};
			let e = carried(&all[i], ctx, 3, None);
			let before = snapshot(&acc);
			assert!(matches!(
				driver.dispatch(&mut acc, &source, &e),
				Err(Fault2::Verify(VerifyError2::EnvelopeContext))
			));
			assert_eq!(snapshot(&acc), before);
		}
	}
}
#[test]
fn carrier_malformed_boundary_precedes_monitor_and_persistent_refusals_leave_monitor_open() {
	let (pin, dep, entries, fixtures) = context();
	let _scope = Root2FixtureScope::install(fixtures);
	let source = CommittedSource2::synthetic();
	let driver = ReplayDriver2 {
		deployment: dep,
		baseline: Baseline2::Provisioned,
		pin: &pin,
	};
	let mut acc = driver.initialize(&source, &entries[3]).unwrap();
	for e in &entries[..3] {
		driver.dispatch(&mut acc, &source, e).unwrap();
	}
	for bytes in [
		vec![1],
		commands(dep)[0]
			.encode(dep.generation, dep.cluster)
			.unwrap(),
	] {
		for side in [None, Some(&[1u8][..])] {
			if bytes.len() > 1 && side.is_none() {
				continue;
			}
			let mut bad = entry(3);
			bad.payload = EntryPayload::Normal(WendelonRequest::Root2Command(
				Root2Carrier1::new(&bytes, side).unwrap(),
			));
			let before = snapshot(&acc);
			assert!(matches!(
				driver.dispatch(&mut acc, &source, &bad),
				Err(Fault2::Carrier)
			));
			assert_eq!(snapshot(&acc), before);
			bad.log_id = entries[1].log_id;
			let prior = ReplayDriver2 {
				deployment: dep,
				baseline: Baseline2::Mark(mark_of2(&acc.state).unwrap()),
				pin: &pin,
			};
			assert!(matches!(
				prior.initialize(&source, &bad),
				Err(Fault2::Carrier)
			));
		}
	}
	// The existing legacy cover can still be observed: the monitor was never latched/advanced.
	driver.dispatch(&mut acc, &source, &entries[3]).unwrap();
}
#[tokio::test(flavor = "current_thread")]
async fn carrier_genesis_acquisition_refusal_is_upstream_of_envelope_and_context() {
	let (pin, dep, _, _) = context();
	let mut e = entry(0);
	e.payload = EntryPayload::Normal(WendelonRequest::Root2Command(
		Root2Carrier1::new(&[1], Some(&[1])).unwrap(),
	));
	let mut reader = store(vec![e.clone()]).await;
	let (_d, mut marks) = completion::marks();
	let durable = marks.durable();
	let result = run(
		&mut reader,
		&Sources(e.log_id),
		caps(),
		dep,
		&pin,
		&mut marks,
	)
	.await;
	crate::storage::mutation_visibility::root2::tests::committed::assert_carrier_missing_genesis(
		&result,
	);
	assert_eq!(marks.durable(), durable);
}

#[tokio::test(flavor = "current_thread")]
async fn carrier_maximum_acquisition_accounts_for_boundary_re_read_without_memory_claim() {
	let mut maximum = entry(1);
	maximum.log_id = LogId::new(CommittedLeaderId::new(u64::MAX, u32::MAX), u64::MAX);
	maximum.payload = EntryPayload::Normal(WendelonRequest::Root2Command(
		Root2Carrier1::new(&vec![0; 262192], Some(&[1; 296])).unwrap(),
	));
	assert_eq!(postcard::to_stdvec(&maximum).unwrap().len(), 262522);
	// Contiguous acquisition starts at genesis, so the actual index here is1; the payload remains maximum.
	maximum.log_id = LogId::new(CommittedLeaderId::new(u64::MAX, u32::MAX), 1);
	let genesis = entry(0);
	let g = postcard::to_stdvec(&genesis).unwrap().len() as u64;
	let n = postcard::to_stdvec(&maximum).unwrap().len() as u64;
	let budget = g + 2 * n;
	for cap in [g + n - 1, budget - 1, budget] {
		let mut reader = store(vec![genesis.clone(), maximum.clone()]).await;
		let result = acquire_committed_prefix2(
			&mut reader,
			&Sources(maximum.log_id),
			AcquisitionCaps2 {
				max_entries: 2,
				max_batch_bytes: cap,
				max_total_bytes: g + n,
			},
			|_| Ok(0usize),
			|count, batch| Ok(count + batch.len()),
		)
		.await;
		if cap == g + n - 1 {
			assert!(matches!(
				result,
				Err(AcquisitionError2::BatchBytes { from: 0 })
			));
		} else if cap == budget - 1 {
			assert!(matches!(
				result,
				Err(AcquisitionError2::BoundaryBytes { from: 0 })
			));
		} else {
			let (count, report) = result.unwrap();
			assert_eq!(count, 2);
			assert_eq!(report.encoded_bytes(), g + n);
		}
	}
	// Byte-cap acceptance above is not a Command2 validity/RSS claim. Replay refuses its zero envelope.
	let (pin, dep, _, _) = context();
	let mut reader = store(vec![genesis, maximum.clone()]).await;
	let (_d, mut marks) = completion::marks();
	let durable = marks.durable();
	let result = run(
		&mut reader,
		&Sources(maximum.log_id),
		AcquisitionCaps2 {
			max_entries: 2,
			max_batch_bytes: budget,
			max_total_bytes: g + n,
		},
		dep,
		&pin,
		&mut marks,
	)
	.await;
	assert!(matches!(result, Err(ReplayError2::Fault(Fault2::Carrier))));
	assert_eq!(marks.durable(), durable);
}
#[test]
fn carrier_sidecar_fingerprint_and_boundary_equality_are_distinct_facts() {
	let (_, dep, _, _) = context();
	let command = Command2::Cover {
		kind: 0,
		external: [1; 32],
	};
	let a = carried(&command, dep, 1, Some(&[2; 296]));
	let b = carried(&command, dep, 1, Some(&[3; 296]));
	let source = CommittedSource2::synthetic();
	let aa = committed_entry2(&source, &a).unwrap();
	let bb = committed_entry2(&source, &b).unwrap();
	let af = aa.committed().entry_fields();
	let bf = bb.committed().entry_fields();
	assert_ne!(af.1, bf.1);
	assert!(af.2 == bf.2);
	crate::storage::mutation_visibility::root2::tests::committed::assert_carrier_boundary_swap(
		&full(),
		&entry(0),
		&a,
		&b,
	);
}
#[test]
fn carrier_wire_version_failure_is_distinct_from_decoded_envelope_and_sidecar_failures() {
	let malformed = Root2Carrier1::new(&[1], None).unwrap();
	let mut wire = postcard::to_stdvec(&malformed).unwrap();
	wire[0] = 2;
	assert_eq!(
		postcard::from_bytes::<Root2Carrier1>(&wire).err(),
		Some(postcard::Error::SerdeDeCustom)
	);
	assert_eq!(
		Root2Carrier1::new(&vec![0; 262193], None).err(),
		Some(Root2CarrierError1::Bound)
	);
	assert_eq!(
		Root2Carrier1::new(&[1], Some(&[1; 297])).err(),
		Some(Root2CarrierError1::Bound)
	);
	let (_, dep, _, fixtures) = context();
	for side in [Some(&[][..]), Some(&[1u8][..]), Some(&[1u8; 295][..])] {
		let e = carried(&fixtures[0].3, dep, 2, side);
		let adapted = committed_entry2(&CommittedSource2::synthetic(), &e).unwrap();
		assert!(adapted.committed().entry_fields().2 == EntryClass2::MalformedRoot2);
		assert!(matches!(command_at(adapted), Err(Fault2::Carrier)));
	}
}

mod refusals;
