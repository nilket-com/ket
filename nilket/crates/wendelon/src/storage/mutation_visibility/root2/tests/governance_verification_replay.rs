//! Raw-carrier dispatcher and acquired-prefix composition, not serving/Raft currentness.
//! The expected producer state is separate from extraction; fixture maps are deliberately wrong.
use super::*;
use crate::consensus::Root2Carrier1;
use crate::storage::mutation_visibility::MutationScope;
use crate::storage::mutation_visibility::root2::governance_command::GovernanceCommandError2;
use crate::storage::mutation_visibility::root2::origin::with_tip_for_test;
use crate::storage::mutation_visibility::root2::recovery::encodings_for_test;
use crate::storage::mutation_visibility::root2::tests::governance_verification::{
	positive_chain, support as g,
};

fn snapshot(a: &Provisional2) -> (Vec<u8>, u64, Progress2, u32) {
	(
		encode_authority2(&a.state).unwrap(),
		a.traversed,
		a.progress,
		encodings_for_test(&a.monitor),
	)
}
fn polluted(entries: &[E]) -> Vec<Fixture> {
	entries
		.iter()
		.filter(|e| {
			matches!(
				e.payload,
				EntryPayload::Normal(WendelonRequest::Root2Command(_))
			)
		})
		.map(|e| {
			let (p, fp) = fields(e);
			(
				p,
				fp,
				EntryClass2::OtherRoot2([8; 32]),
				Command2::Cover {
					kind: 0,
					external: [9; 32],
				},
			)
		})
		.collect()
}
fn prefix<'a>(
	f: &'a g::Fixture,
	boundary: &E,
	n: usize,
) -> (ReplayDriver2<'a>, CommittedSource2, Provisional2) {
	let driver = ReplayDriver2 {
		deployment: f.dep,
		baseline: Baseline2::Provisioned,
		pin: &f.pin,
	};
	let source = CommittedSource2::synthetic();
	let mut a = driver.initialize(&source, boundary).unwrap();
	for e in &f.entries[..n] {
		driver.dispatch(&mut a, &source, e).unwrap();
	}
	(driver, source, a)
}
async fn assert_refused_run(f: &g::Fixture, e: E, want: VerifyError2) {
	let mut entries = f.entries.clone();
	entries.push(e);
	let (d, mut marks) = completion::marks();
	let old = std::fs::read(d.as_ref().join("RECOVERY_MARK")).unwrap();
	let mut reader = store(entries.clone()).await;
	assert!(
		matches!(run(&mut reader,&Sources(entries.last().unwrap().log_id),caps(),f.dep,&f.pin,&mut marks).await,Err(ReplayError2::Fault(Fault2::Verify(got))) if got==want)
	);
	assert_eq!(marks.durable(), Some(Baseline2::Provisioned));
	assert_eq!(
		std::fs::read(d.as_ref().join("RECOVERY_MARK")).unwrap(),
		old
	);
}
async fn assert_denied_run(f: &g::Fixture, c: Command2, side: Vec<u8>) {
	let e = g::carrier(&c, f.dep, g::tip(&f.state).index + 1, Some(&side));
	let (expected, out) = g::step(&f.state, &f.pin, f.dep, &e, &c, Some(&side)).unwrap();
	assert!(matches!(
		out,
		ApplyResult2::Denied {
			reason: 8,
			reference: None
		}
	));
	let ce = committed_entry2(&CommittedSource2::synthetic(), &e)
		.unwrap()
		.into_committed();
	let (p, fp, _) = ce.entry_fields();
	let digest = c.digest(f.dep.generation, f.dep.cluster).unwrap();
	assert_eq!(
		g::bytes(&expected),
		g::bytes(&with_tip_for_test(
			&f.state,
			Log2 {
				term: p.term,
				node: p.node,
				index: p.index
			},
			fp,
			digest
		))
	);
	let mut entries = f.entries.clone();
	entries.push(e.clone());
	let _scope = Root2FixtureScope::install(polluted(&entries));
	let (driver, source, mut a) = prefix(f, &e, f.entries.len());
	assert_eq!(g::bytes(&a.state), g::bytes(&f.state));
	let before = a.traversed;
	driver.dispatch(&mut a, &source, &e).unwrap();
	assert_eq!(a.traversed, before + 1);
	assert!(matches!(a.progress, Progress2::Ready(_)));
	assert_eq!(g::bytes(&a.state), g::bytes(&expected));
	let (_d, mut marks) = completion::marks();
	let mut reader = store(entries.clone()).await;
	let output = run(
		&mut reader,
		&Sources(e.log_id),
		caps(),
		f.dep,
		&f.pin,
		&mut marks,
	)
	.await
	.unwrap();
	assert_eq!(g::bytes(output.state()), g::bytes(&expected));
	assert_eq!(marks.durable(), Some(Baseline2::Mark(output.mark())));
}
#[tokio::test(flavor = "current_thread")]
#[cfg(target_endian = "little")]
async fn s3_all_nine_recover_real_wal_and_replay_sidecars_bypassing_both_fixture_maps() {
	let f = positive_chain();
	let _scope = Root2FixtureScope::install(polluted(&f.entries));
	let directory = crate::test_support::TestDir::new("s3-real-carrier-wal");
	let open = || {
		WendelonLogStore::open_with_file_ops(
			directory.as_ref().join("wal"),
			directory.as_ref().join("vote"),
			directory.as_ref().join("snapshot"),
			std::sync::Arc::new(crate::storage::file_ops::BlockingFileOps),
		)
	};
	let mut writer = open().await.unwrap();
	writer.blocking_append(f.entries.clone()).await.unwrap();
	drop(writer);
	let mut reader = open().await.unwrap();
	let recovered = reader.try_get_log_entries(0..=13).await.unwrap();
	assert_eq!(
		postcard::to_stdvec(&recovered).unwrap(),
		postcard::to_stdvec(&f.entries).unwrap()
	);
	let (_d, mut marks) = completion::marks();
	let output = run(
		&mut reader,
		&Sources(f.entries.last().unwrap().log_id),
		caps(),
		f.dep,
		&f.pin,
		&mut marks,
	)
	.await
	.unwrap();
	assert_eq!(g::bytes(output.state()), g::bytes(&f.state));
	assert_eq!(output.report().entries(), 14);
	assert_eq!(output.mark(), mark_of2(&f.state).unwrap());
	assert_eq!(marks.durable(), Some(Baseline2::Mark(output.mark())));
}
#[tokio::test(flavor = "current_thread")]
#[cfg(target_endian = "little")]
async fn s3_all_nine_raw_missing_and_malformed_refusals_leave_accumulator_and_marks() {
	let f = g::Fixture::new("b");
	let boundary = g::entry(4);
	for c in g::nine(&f) {
		for mode in 0..4 {
			let mut side = f.envelope(&f.state, &c, 0x92);
			if mode == 2 {
				side[0] ^= 1;
			}
			if mode == 3 {
				side[12..28].fill(0);
			}
			let e = g::carrier(
				&c,
				f.dep,
				3,
				if mode == 0 {
					None
				} else if mode == 1 {
					Some(&side[..295])
				} else {
					Some(&side)
				},
			);
			let (driver, source, mut a) = prefix(&f, &boundary, 3);
			let before = snapshot(&a);
			let result = driver.dispatch(&mut a, &source, &e);
			if mode == 1 {
				assert!(matches!(result, Err(Fault2::Carrier)));
				let mut entries = f.entries.clone();
				entries.push(e);
				let (d, mut marks) = completion::marks();
				let old = std::fs::read(d.as_ref().join("RECOVERY_MARK")).unwrap();
				let mut reader = store(entries.clone()).await;
				assert!(matches!(
					run(
						&mut reader,
						&Sources(entries.last().unwrap().log_id),
						caps(),
						f.dep,
						&f.pin,
						&mut marks
					)
					.await,
					Err(ReplayError2::Fault(Fault2::Carrier))
				));
				assert_eq!(marks.durable(), Some(Baseline2::Provisioned));
				assert_eq!(
					std::fs::read(d.as_ref().join("RECOVERY_MARK")).unwrap(),
					old
				);
			} else {
				let want = if mode == 0 {
					VerifyError2::MissingSidecar
				} else {
					if mode == 2 {
						VerifyError2::Governance(GovernanceCommandError2::Magic)
					} else {
						VerifyError2::Governance(GovernanceCommandError2::ZeroField(crate::storage::mutation_visibility::root2::governance_command::GovernanceField2::Generation))
					}
				};
				assert!(matches!(result,Err(Fault2::Verify(got)) if got==want));
				assert_refused_run(&f, e, want).await;
			}
			assert_eq!(snapshot(&a), before);
		}
	}
}
#[test]
#[cfg(target_endian = "little")]
fn s3_all_nine_raw_state_context_position_and_structure_first_errors() {
	let f = g::Fixture::new("b");
	let boundary = g::entry(4);
	for c in g::nine(&f) {
		for n in [0, 2, 3] {
			let (driver, source, mut a) = prefix(&f, &boundary, n);
			let before = snapshot(&a);
			let e = g::carrier(&c, f.dep, 3, None);
			let want = if n == 3 {
				VerifyError2::MissingSidecar
			} else {
				VerifyError2::StateKind
			};
			assert!(
				matches!(driver.dispatch(&mut a,&source,&e),Err(Fault2::Verify(got)) if got==want)
			);
			assert_eq!(snapshot(&a), before);
			let mut zero = g::carrier(&c, f.dep, 3, None);
			zero.log_id = LogId::default();
			assert!(matches!(
				driver.dispatch(&mut a, &source, &zero),
				Err(Fault2::Bind(BindError2::Position))
			));
			assert_eq!(snapshot(&a), before);
			let mut other = f.dep;
			other.cluster[0] ^= 1;
			let mut wrong = g::carrier(&c, other, 3, None);
			wrong.log_id = LogId::default();
			assert!(matches!(
				driver.dispatch(&mut a, &source, &wrong),
				Err(Fault2::Verify(VerifyError2::EnvelopeContext))
			));
			assert_eq!(snapshot(&a), before);
			wrong.payload = EntryPayload::Normal(WendelonRequest::Root2Command(
				Root2Carrier1::new(
					&c.encode(other.generation, other.cluster).unwrap(),
					Some(&[0u8; 295]),
				)
				.unwrap(),
			));
			assert!(matches!(
				driver.dispatch(&mut a, &source, &wrong),
				Err(Fault2::Carrier)
			));
			assert_eq!(snapshot(&a), before);
		}
	}
}
#[tokio::test(flavor = "current_thread")]
#[cfg(target_endian = "little")]
async fn s3_raw_consumed_denials_scope_and_resigned_issuer_failures_advance_only_coverage() {
	let f = g::Fixture::new("a");
	for c in g::nine(&f) {
		let side = f.envelope(&f.state, &c, 0x92);
		assert_denied_run(&f, c, side).await;
	}
	let f = g::Fixture::new("b");
	let c = Command2::EnrollSelector(g::selector(&f));
	let ctx = crate::storage::mutation_visibility::root2::admin_install::admin_context2(
		&crate::storage::mutation_visibility::root2::admin_install::install_admin2(
			&f.state, &f.pin,
		)
		.unwrap(),
	);
	let digest = c.digest(f.dep.generation, f.dep.cluster).unwrap();
	for i in 0..8 {
		let mut context = ctx;
		match i {
			0 => context.generation[0] ^= 1,
			1 => context.cluster[0] ^= 1,
			2 => context.installed_at.index += 1,
			3 => context.bootstrap_fingerprint[0] ^= 1,
			4 => context.bootstrap_command[0] ^= 1,
			5 => context.genesis[0] ^= 1,
			6 => context.initial_epoch += 1,
			_ => context.manifest_pin[0] ^= 1,
		}
		assert_denied_run(&f, c.clone(), g::sign_context(context, digest, 0x92)).await;
	}
	let mut wrong = digest;
	wrong[0] ^= 1;
	assert_denied_run(&f, c.clone(), g::sign_context(ctx, wrong, 0x92)).await;
	for seed in [0x42, 0x93] {
		assert_denied_run(&f, c.clone(), f.envelope(&f.state, &c, seed)).await;
	}
	let mut noncanonical = f.envelope(&f.state, &c, 0x92);
	noncanonical[264..296].fill(0xff);
	assert_denied_run(&f, c, noncanonical).await;
	let f = g::Fixture::table_namespace();
	for i in 0..5 {
		let mut id = g::identity(&f);
		match i {
			0 => id.generation[0] ^= 1,
			1 => id.tenant += 1,
			2 => {
				id.scope = MutationScope::Raw;
				id.table = [0; 16];
				id.schema_generation = 0;
			}
			3 => id.table[0] ^= 1,
			_ => id.schema_generation += 1,
		}
		let c = Command2::Resolve {
			identity: id,
			digest: [0x31; 32],
			reservation: g::log(3),
			cause: 1,
			anchor: g::log(2),
		};
		let side = f.envelope(&f.state, &c, 0x92);
		assert_denied_run(&f, c, side).await;
	}
	let mut f = g::Fixture::new("b");
	let c = g::grant(&f, g::ISSUER_SEED);
	let side = f.envelope(&f.state, &c, 0x92);
	assert_denied_run(&f, c, side).await;
	f.governed(Command2::EnrollLeaseIssuer {
		node: 7,
		key: g::key(g::ISSUER_SEED),
	});
	for mode in 0..3 {
		let mut c = g::grant(&f, if mode == 0 { 0x34 } else { g::ISSUER_SEED });
		let Command2::Grant { lease, .. } = &mut c else {
			unreachable!()
		};
		if mode == 1 {
			lease.expiry.physical_ms += 1;
		}
		if mode == 2 {
			lease.signature[32..].fill(0xff);
		}
		let side = f.envelope(&f.state, &c, 0x92);
		assert_denied_run(&f, c, side).await;
	}
	f.governed(Command2::RevokeLeaseIssuer { node: 7 });
	let c = g::grant(&f, g::ISSUER_SEED);
	let side = f.envelope(&f.state, &c, 0x92);
	assert_denied_run(&f, c, side).await;
}

#[tokio::test(flavor = "current_thread")]

async fn s3_raw_reused_envelope_at_next_coordinates_preserves_first_registration() {
	let mut f = g::Fixture::new("b");
	let c = Command2::EnrollSelector(g::selector(&f));
	let side = f.envelope(&f.state, &c, 0x92);
	assert_eq!(
		f.apply(c.clone(), Some(side.clone())),
		ApplyResult2::Registered(g::log(3))
	);
	assert_eq!(
		f.apply(c.clone(), Some(side)),
		ApplyResult2::Registered(g::log(3))
	);
	let _scope = Root2FixtureScope::install(polluted(&f.entries));
	let (_d, mut marks) = completion::marks();
	let mut reader = store(f.entries.clone()).await;
	let out = run(
		&mut reader,
		&Sources(f.entries.last().unwrap().log_id),
		caps(),
		f.dep,
		&f.pin,
		&mut marks,
	)
	.await
	.unwrap();
	assert_eq!(g::bytes(out.state()), g::bytes(&f.state));
	assert_eq!(g::tip(out.state()), g::log(4));
	assert_eq!(
		crate::storage::mutation_visibility::root2::origin::root_and_origin2(out.state())
			.unwrap()
			.0
			.state
			.selectors
			.len(),
		1
	);
	assert_eq!(marks.durable(), Some(Baseline2::Mark(out.mark())));
}
