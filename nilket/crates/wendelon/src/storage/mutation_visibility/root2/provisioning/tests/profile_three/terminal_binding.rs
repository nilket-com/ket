//! Pure matrix and physical record boundaries; no authority or runtime claims.
use super::*;
use crate::storage::mutation_visibility::root2::{
	admin::record::BootstrapObservation2,
	provisioning::{inventory::*, owner_core::*, terminal::*},
	types::Log2,
};
/// The profile-3 root (the `p3_events` Reserve and Approve) in `state`; Retired is reached only by
/// the committed terminal Retire (`terminal_event`).
fn prefix(state: RootState2) -> (Vec<u8>, RootIdentity2, TerminalObservation3) {
	let v = vectors();
	let mut reserve = inventory::v2::decode_event2(&b(&v["p3_events"][0])).unwrap();
	reserve.seq = 1;
	reserve.op_id = [1; 16];
	let mut approve = inventory::v2::decode_event2(&b(&v["p3_events"][1])).unwrap();
	approve.seq = 2;
	approve.op_id = [2; 16];
	let id = approve.identity.unwrap();
	let EventBody2::Approve { record } = &mut approve.body else {
		panic!("approve")
	};
	record.inventory_seq = 2;
	let obs = TerminalObservation3 {
		identity: record.identity,
		original_admin_key: id.admin_key,
		at: Log2 {
			term: 1,
			node: 1,
			index: 9,
		},
		fingerprint: [9; 32],
		command_digest: [10; 32],
		state_digest: [11; 32],
	};
	let mut bytes = inventory::v2::encode_event2(&reserve).unwrap().to_vec();
	match state {
		RootState2::Prepared => {}
		RootState2::Bound => bytes.extend(inventory::v2::encode_event2(&approve).unwrap()),
		RootState2::Conflicted => {
			let mut observation = BootstrapObservation2 {
				identity: obs.identity,
			};
			observation.identity.initial_epoch += 1;
			let e = Event2 {
				seq: 2,
				op_id: [2; 16],
				identity: Some(id),
				body: EventBody2::Conflict { observation },
			};
			bytes.extend(inventory::v2::encode_event2(&e).unwrap());
		}
		RootState2::Retired => unreachable!("a Retired prefix is the terminal Retire"),
	}
	inventory::v2::fold_events2(&bytes).unwrap();
	(bytes, id, obs)
}
fn terminal_event() -> (Vec<u8>, Event2) {
	let (bytes, id, obs) = prefix(RootState2::Bound);
	let Decision2::Append(event) = owner_core::v2::decide2(
		&bytes,
		&op(9),
		&OwnerRequest2::Retire {
			identity: id,
			terminal: obs,
		},
	) else {
		panic!("append")
	};
	(bytes, event)
}
fn reseal(mut bytes: Vec<u8>) -> Vec<u8> {
	let n = bytes.len() - 32;
	let sum = hash(b"", &bytes[..n]);
	bytes[n..].copy_from_slice(&sum);
	bytes
}
#[test]
fn terminal_v2_payload_lengths_padding_checksum_and_fold_sequence() {
	let (prefix, event) = terminal_event();
	let raw = inventory::v2::encode_event2(&event).unwrap();
	assert_eq!(inventory::v2::decode_event2(&raw).unwrap(), event);
	for n in 1..=336 {
		if n == 156 {
			continue;
		}
		let mut bad = raw.to_vec();
		bad[132..134].copy_from_slice(&(n as u16).to_le_bytes());
		assert!(
			inventory::v2::decode_event2(&reseal(bad)).is_err(),
			"length {n}"
		);
	}
	for offset in [134, 143, 144 + 156, 479] {
		let mut bad = raw.to_vec();
		bad[offset] = 1;
		assert!(
			inventory::v2::decode_event2(&reseal(bad)).is_err(),
			"offset {offset}"
		);
	}
	let mut bad = raw;
	bad[480] ^= 1;
	assert!(inventory::v2::decode_event2(&bad).is_err());
	let mut bad = event.clone();
	if let EventBody2::RetireTerminal { terminal } = &mut bad.body {
		terminal.inventory_seq += 1;
	}
	let b = inventory::v2::encode_event2(&bad).unwrap();
	assert!(inventory::v2::fold_events2(&[prefix.as_slice(), b.as_slice()].concat()).is_err());
}
#[test]
fn terminal_event_and_snapshot_match_independent_python_vectors() {
	let v: serde_json::Value = serde_json::from_str(include_str!("terminal_vectors.json")).unwrap();
	let (prefix, event) = terminal_event();
	let raw = inventory::v2::encode_event2(&event).unwrap();
	assert_eq!(raw.as_slice(), b(&v["event"]));
	let folded =
		inventory::v2::fold_events2(&[prefix.as_slice(), raw.as_slice()].concat()).unwrap();
	assert_eq!(
		snapshot::encode_snapshot2(&folded).unwrap(),
		b(&v["snapshot"])
	);
}
/// The profile-3 rows of the matrix with the Retired prefix reached by the committed terminal
/// Retire (not a plain Retire): each state and observation option gets its profile-3 decision.
#[test]
fn terminal_profile_three_matrix_with_a_terminal_retired_prefix() {
	for state in [
		RootState2::Prepared,
		RootState2::Bound,
		RootState2::Conflicted,
		RootState2::Retired,
	] {
		let (bytes, id, obs) = if state == RootState2::Retired {
			let (prefix, event) = terminal_event();
			let (_, id, obs) = prefix_parts();
			let raw = inventory::v2::encode_event2(&event).unwrap();
			([prefix.as_slice(), raw.as_slice()].concat(), id, obs)
		} else {
			prefix(state)
		};
		let folded = inventory::v2::fold_events2(&bytes).unwrap();
		assert_eq!(folded.roots[0].state, state);
		assert_eq!(
			folded.roots[0].terminal.is_some(),
			state == RootState2::Retired
		);
		// A Retire always carries its committed observation (the request type requires it).
		let q = OwnerRequest2::Retire {
			identity: id,
			terminal: obs,
		};
		let result = owner_core::v2::decide2(&bytes, &op(9), &q);
		match state {
			// The recorded terminal retirement: its own observation is a no-op.
			RootState2::Retired => {
				assert_eq!(result, Decision2::NoOp(RootState2::Retired));
				// Any other observation is not the recorded retirement.
				let mut other = obs;
				other.state_digest[0] ^= 1;
				let q = OwnerRequest2::Retire {
					identity: id,
					terminal: other,
				};
				assert_eq!(
					owner_core::v2::decide2(&bytes, &op(9), &q),
					Decision2::Refuse(OwnerError2::RetirementMismatch)
				);
			}
			RootState2::Bound => assert!(matches!(result, Decision2::Append(_))),
			_ => assert_eq!(result, Decision2::Refuse(OwnerError2::NotBound)),
		}
	}
}
fn prefix_parts() -> (Vec<u8>, RootIdentity2, TerminalObservation3) {
	prefix(RootState2::Bound)
}
