use super::*;

#[tokio::test(flavor = "current_thread")]
async fn carrier_structural_context_kind_and_extra_refusals_never_update_durable_mark() {
	for case in 0..7 {
		let (pin, dep, mut entries, _) = real_context();
		let all = commands(dep);
		let mut ctx = dep;
		if case == 0 {
			ctx.generation[0] ^= 1;
		}
		let (command, side, index) = match case {
			0 => (&all[2], None, 3),
			1 => (&all[1], Some(&[1u8; 296][..]), 3),
			2 => (&all[10], None, 3),
			3 => (&all[4], None, 3),
			4 => (&all[0], None, 3),
			5 => (&all[2], None, 1),
			_ => (&all[10], Some(&[1u8][..]), 3),
		};
		entries[index] = carried(command, ctx, index as u64, side);
		let (d, mut marks) = completion::marks();
		let durable = marks.durable();
		let bytes = std::fs::read(d.as_ref().join("RECOVERY_MARK")).unwrap();
		let mut reader = store(entries.clone()).await;
		let result = run(
			&mut reader,
			&Sources(entries[3].log_id),
			caps(),
			dep,
			&pin,
			&mut marks,
		)
		.await;
		match case {
			0 => assert!(matches!(
				result,
				Err(ReplayError2::Fault(Fault2::Verify(
					VerifyError2::EnvelopeContext
				)))
			)),
			1 => assert!(matches!(
				result,
				Err(ReplayError2::Fault(Fault2::Verify(
					VerifyError2::ExtraSidecar
				)))
			)),
			2 => assert!(matches!(
				result,
				Err(ReplayError2::Fault(Fault2::Unsupported(
					UnsupportedKind2::Cut
				)))
			)),
			3 => assert!(matches!(
				result,
				Err(ReplayError2::Fault(Fault2::Verify(
					VerifyError2::NotApplicable
				)))
			)),
			4 | 5 => assert!(matches!(
				result,
				Err(ReplayError2::Fault(Fault2::Verify(VerifyError2::StateKind)))
			)),
			_ => assert!(matches!(result, Err(ReplayError2::Fault(Fault2::Carrier)))),
		}
		assert_eq!(marks.durable(), durable);
		assert_eq!(
			std::fs::read(d.as_ref().join("RECOVERY_MARK")).unwrap(),
			bytes
		);
	}
}

#[test]
fn carrier_bad_sidecar_precedes_context_position_and_all_governance_kinds() {
	let (_, dep, _, _) = context();
	let all = commands(dep);
	for command in &all[5..] {
		let (pin, dep, entries, fixtures) = context();
		let _scope = Root2FixtureScope::install(fixtures);
		let source = CommittedSource2::synthetic();
		let driver = ReplayDriver2 {
			deployment: dep,
			baseline: Baseline2::Provisioned,
			pin: &pin,
		};
		let mut acc = driver.initialize(&source, &entries[3]).unwrap();
		driver.dispatch(&mut acc, &source, &entries[0]).unwrap();
		let mut wrong = dep;
		wrong.cluster[0] ^= 1;
		let mut e = carried(command, wrong, 1, Some(&[1; 295]));
		e.log_id = LogId::default();
		let before = snapshot(&acc);
		assert!(matches!(
			driver.dispatch(&mut acc, &source, &e),
			Err(Fault2::Carrier)
		));
		assert_eq!(snapshot(&acc), before);
	}
}
