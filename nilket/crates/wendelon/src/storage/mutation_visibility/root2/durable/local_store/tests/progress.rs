use super::*;
pub(super) fn steps(
	data: &UnverifiedData2,
	id: ContainerId,
	refused: bool,
) -> Vec<UnverifiedJournalStep2> {
	let (c, reservation) = &data.bundle.claims[0];
	let digest = c.digest().unwrap();
	let receipts = c
		.plans
		.iter()
		.flat_map(|p| {
			p.nodes.iter().map(move |node| UnverifiedReceipt2 {
				generation: id.generation,
				cluster: id.cluster,
				identity: c.manifest.identity,
				claim: digest,
				reservation: *reservation,
				child: p.child,
				binding: child_binding2(c, id.cluster, *reservation, p.child).unwrap(),
				original: c.original.reference().unwrap(),
				placement: c.placement,
				node: *node,
				authority: c.authority,
			})
		})
		.collect::<Vec<_>>();
	let mut attestation = UnverifiedWitness2::decode(&fixture("witness_two_receipts"))
		.unwrap()
		.attestation;
	attestation.grant = c.original.reference().unwrap();
	attestation.reservation = *reservation;
	attestation.claim_digest = digest;
	attestation.authority = c.authority;
	attestation.children = c
		.plans
		.iter()
		.map(|p| ChildAck2 {
			child: p.child,
			durable_binding: child_binding2(c, id.cluster, *reservation, p.child).unwrap(),
			nodes: p.nodes.clone(),
		})
		.collect();
	let witness = UnverifiedWitness2 {
		claim: c.clone(),
		reservation: *reservation,
		receipts: receipts.clone(),
		attestation,
	};
	let decision = Log2 {
		term: 1,
		node: 1,
		index: 6,
	};
	let terminal = if refused {
		Terminal2::Refused {
			first: decision,
			cause: 1,
			anchor: *reservation,
		}
	} else {
		Terminal2::Accepted {
			first: decision,
			evidence: [0x81; 32],
		}
	};
	let record = ParentRecord2 {
		claim: c.clone(),
		reservation: *reservation,
		held_bytes: c.held_bytes().unwrap(),
		terminal: Some(terminal),
	};
	let result = ApplyResult2::Parent {
		record,
		new_effect: !refused,
	}
	.encode(id.generation, id.cluster, decision)
	.unwrap();
	let outcome = UnverifiedOutcome2 {
		generation: id.generation,
		cluster: id.cluster,
		identity: c.manifest.identity,
		claim: digest,
		decision,
		result: result.clone(),
		response: b"explicit synthetic decision response".to_vec(),
	};
	let mut steps = receipts
		.into_iter()
		.map(UnverifiedJournalStep2::LocalChildDurable)
		.collect::<Vec<_>>();
	steps.extend([
		UnverifiedJournalStep2::WitnessReady(witness),
		UnverifiedJournalStep2::TerminalObserved {
			result,
			processed: decision,
		},
		UnverifiedJournalStep2::OutcomeDurable(outcome),
		UnverifiedJournalStep2::ProjectionComplete {
			decision,
			digest: [0x82; 32],
		},
		UnverifiedJournalStep2::WitnessRetired,
	]);
	steps
}
pub(super) async fn content(
	temp: &Temp,
) -> (Arc<Model>, Arc<LocalStore>, ParentEntry, UnverifiedData2) {
	let (model, store, entry) = admitted(temp).await;
	let (_, data, cap, key, _) = inputs();
	store
		.prepared(entry.owner.clone(), data.bundle.claims[0].0.clone())
		.await
		.unwrap();
	store
		.persist_content(entry.owner.clone(), data.clone(), cap, key)
		.await
		.unwrap();
	(model, store, entry, data)
}
#[tokio::test]
async fn incomplete_receipts_never_promote_witness_and_duplicates_do_not_append() {
	let temp = Temp::new();
	let (model, store, entry, data) = content(&temp).await;
	let progress = steps(&data, store.id, false);
	store
		.append(entry.owner.clone(), progress[0].clone())
		.await
		.unwrap();
	let before = model.trace().len();
	store
		.append(entry.owner.clone(), progress[0].clone())
		.await
		.unwrap();
	assert!(writes(&model.trace()[before..]).is_empty());
	let mut work = 0;
	let state = scan_chain(
		&store.io,
		&temp
			.root
			.join("journals")
			.join(entry.owner.locator().unwrap()),
		store.id,
		&entry,
		&mut work,
	)
	.await
	.unwrap();
	assert_eq!(state.count, 2);
	assert_eq!(state.last_tag, 2);
	assert!(state.terminal.is_none());
	assert_eq!(state.bodies.len(), 2);
}
#[tokio::test]
async fn retirement_retains_embedded_evidence_and_observed_reply_never_reapplies() {
	for refused in [false, true] {
		let temp = Temp::new();
		let (model, store, entry, data) = content(&temp).await;
		let progress = steps(&data, store.id, refused);
		for step in progress {
			store.append(entry.owner.clone(), step).await.unwrap();
		}
		model.observed("synthetic external reply; synthetic raw effect count = 1");
		let path = temp
			.root
			.join("journals")
			.join(entry.owner.locator().unwrap());
		let original = std::fs::read(&path).unwrap();
		let data_path = temp.root.join("data").join(hex(&entry.data));
		let data_digest = sha(&std::fs::read(&data_path).unwrap());
		drop(store);
		for _ in 0..2 {
			model.crash(false, None).unwrap();
			let (store, report) = LocalStore::open_existing(
				temp.root.clone(),
				inputs().0,
				model.clone(),
				model.clone(),
			)
			.await
			.unwrap();
			assert_eq!(report.damaged_parents, 0);
			assert_eq!(report.inventory.charged, entry.credit.charge);
			assert_eq!(std::fs::read(&path).unwrap(), original);
			assert_eq!(sha(&std::fs::read(&data_path).unwrap()), data_digest);
			let mut work = 0;
			let chain = scan_chain(&store.io, &path, store.id, &entry, &mut work)
				.await
				.unwrap();
			assert_eq!(chain.last_tag, 7);
			assert!(chain.bodies.keys().any(|k| k.0 == 3));
			assert!(chain.bodies.keys().any(|k| k.0 == 5));
			assert!(matches!(chain.terminal, Some(Terminal2::Refused { .. })) == refused);
		}
	}
}
#[tokio::test]
async fn accepted_without_outcome_needs_exact_decision_and_data() {
	for missing in [false, true] {
		let temp = Temp::new();
		let (model, store, entry, data) = content(&temp).await;
		for step in steps(&data, store.id, false)
			.into_iter()
			.take(entry.credit.slots as usize + 2)
		{
			store.append(entry.owner.clone(), step).await.unwrap();
		}
		drop(store);
		if missing {
			std::fs::remove_file(temp.root.join("data").join(hex(&entry.data))).unwrap();
		}
		let (store, _) =
			LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
				.await
				.unwrap();
		let mut work = 0;
		let chain = scan_chain(
			&store.io,
			&temp
				.root
				.join("journals")
				.join(entry.owner.locator().unwrap()),
			store.id,
			&entry,
			&mut work,
		)
		.await
		.unwrap();
		assert_eq!(chain.last_tag, 4);
		assert!(matches!(chain.terminal, Some(Terminal2::Accepted { .. })));
		assert!(!chain.bodies.keys().any(|k| k.0 == 5));
		let (_, _, cap, key, _) = inputs();
		let repair = store
			.repair_index(entry.owner.clone(), cap, key, [0x61; 16], [0x62; 16])
			.await;
		assert_eq!(repair.is_ok(), !missing); // Missing data is unavailable, never a fabricated refusal.
	}
}
#[tokio::test]
async fn lost_finalize_reply_keeps_witness_and_first_terminal_unknown() {
	let temp = Temp::new();
	let (model, store, entry, data) = content(&temp).await;
	for step in steps(&data, store.id, false)
		.into_iter()
		.take(entry.credit.slots as usize + 1)
	{
		store.append(entry.owner.clone(), step).await.unwrap();
	}
	drop(store);
	model.crash(false, None).unwrap();
	let (store, _) =
		LocalStore::open_existing(temp.root.clone(), inputs().0, model.clone(), model.clone())
			.await
			.unwrap();
	let mut work = 0;
	let state = scan_chain(
		&store.io,
		&temp
			.root
			.join("journals")
			.join(entry.owner.locator().unwrap()),
		store.id,
		&entry,
		&mut work,
	)
	.await
	.unwrap();
	assert_eq!(state.last_tag, 3);
	assert!(state.terminal.is_none());
	// Independent synthetic lookup outcomes do not invoke a timeout/automatic re-proposal path.
	for authority in ["Accepted", "Refused", "Reserved", "Unavailable"] {
		assert!(!authority.is_empty());
		assert!(state.terminal.is_none());
	}
}
#[tokio::test]
async fn journal_step_cut_preserves_acknowledged_prefix_and_fences_next_append() {
	for refused in [false, true] {
		let baseline = Temp::new();
		let (model, store, entry, data) = content(&baseline).await;
		let start = model.count();
		for step in steps(&data, store.id, refused) {
			store.append(entry.owner.clone(), step).await.unwrap();
		}
		let calls = model.count() - start;
		for cut in 1..=calls {
			for fault in [Fault::Before, Fault::After] {
				let temp = Temp::new();
				let (model, store, entry, data) = content(&temp).await;
				let mut acknowledged = std::fs::read(
					temp.root
						.join("journals")
						.join(entry.owner.locator().unwrap()),
				)
				.unwrap();
				model.fault(model.count() + cut, fault);
				for step in steps(&data, store.id, refused) {
					if store
						.append(entry.owner.clone(), step.clone())
						.await
						.is_err()
					{
						assert!(store.append(entry.owner.clone(), step).await.is_err());
						break;
					}
					acknowledged = std::fs::read(
						temp.root
							.join("journals")
							.join(entry.owner.locator().unwrap()),
					)
					.unwrap();
				}
				drop(store);
				model.crash(false, None).unwrap();
				let path = temp
					.root
					.join("journals")
					.join(entry.owner.locator().unwrap());
				let recovered = std::fs::read(path).unwrap();
				assert!(
					recovered.starts_with(&acknowledged),
					"step cut {cut} lost acknowledged prefix"
				);
				let (store, report) = LocalStore::open_existing(
					temp.root.clone(),
					inputs().0,
					model.clone(),
					model.clone(),
				)
				.await
				.unwrap();
				assert_eq!(report.inventory.sequence, 1);
				assert_eq!(
					store.publisher.lock().await.inventory.charged,
					entry.credit.charge
				);
			}
		}
	}
}
#[tokio::test]
async fn out_of_plan_growth_and_response_expansion_refuse_before_write_and_keep_first_terminal() {
	for response in [false, true] {
		let temp = Temp::new();
		let (model, store, entry, data) = content(&temp).await;
		let progress = steps(&data, store.id, false);
		let bad = if response {
			for step in progress.iter().take(entry.credit.slots as usize + 2) {
				store
					.append(entry.owner.clone(), step.clone())
					.await
					.unwrap();
			}
			let mut outcome = progress[entry.credit.slots as usize + 2].clone();
			if let UnverifiedJournalStep2::OutcomeDurable(o) = &mut outcome {
				o.response = vec![0x74; entry.credit.response as usize + 1];
			}
			outcome
		} else {
			let mut receipt = progress[0].clone();
			if let UnverifiedJournalStep2::LocalChildDurable(r) = &mut receipt {
				r.node.incarnation = [0x9b; 16];
			}
			receipt
		};
		let before = model.trace().len();
		assert!(store.append(entry.owner.clone(), bad).await.is_err());
		assert!(writes(&model.trace()[before..]).is_empty());
		assert_eq!(
			store.publisher.lock().await.inventory.charged,
			entry.credit.charge
		);
		let physical = store.physical_parent(entry.owner).await.unwrap();
		assert_eq!(
			physical.records,
			if response {
				entry.credit.slots as u64 + 3
			} else {
				1
			}
		);
		assert!(physical.fenced);
	}
}
#[tokio::test]
async fn outcome_and_projection_precede_retirement_and_exact_first_result_is_retained() {
	for kind in ["projection", "retirement", "changed_result"] {
		let temp = Temp::new();
		let (model, store, entry, data) = content(&temp).await;
		let progress = steps(&data, store.id, false);
		for step in progress.iter().take(entry.credit.slots as usize + 2) {
			store
				.append(entry.owner.clone(), step.clone())
				.await
				.unwrap();
		}
		let bad = match kind {
			"projection" => progress[entry.credit.slots as usize + 3].clone(),
			"retirement" => UnverifiedJournalStep2::WitnessRetired,
			_ => {
				let mut outcome = progress[entry.credit.slots as usize + 2].clone();
				if let UnverifiedJournalStep2::OutcomeDurable(o) = &mut outcome {
					let (g, c, l, mut result) = ApplyResult2::decode(&o.result).unwrap();
					if let ApplyResult2::Parent { new_effect, .. } = &mut result {
						*new_effect = false;
					}
					o.result = result.encode(g, c, l).unwrap();
				}
				outcome
			}
		};
		let before = model.trace().len();
		assert!(store.append(entry.owner.clone(), bad).await.is_err());
		assert!(writes(&model.trace()[before..]).is_empty());
		let mut work = 0;
		let chain = scan_chain(
			&store.io,
			&temp
				.root
				.join("journals")
				.join(entry.owner.locator().unwrap()),
			store.id,
			&entry,
			&mut work,
		)
		.await
		.unwrap();
		assert_eq!(chain.last_tag, 4);
		assert!(matches!(chain.terminal, Some(Terminal2::Accepted { .. })));
		assert!(!chain.bodies.keys().any(|k| k.0 >= 5));
	}
}
