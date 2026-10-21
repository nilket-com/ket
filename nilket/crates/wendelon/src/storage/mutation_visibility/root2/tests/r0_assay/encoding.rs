//! Current-wire oracle comparisons; direct fixtures are synthetic, never trusted replay.
use super::*;
use crate::storage::mutation_visibility::MutationScope;
use crate::storage::mutation_visibility::root2::admin_statement::{
	AdmissionNamespace2, PermissionKey2,
};
use crate::storage::mutation_visibility::root2::admission_registry::*;
use crate::storage::mutation_visibility::root2::state::encode_state_reference;

fn equality(r: &Root2) {
	let before = encode_state_reference(&r.state).unwrap();
	let after = r.encode().unwrap();
	assert_eq!(after, before);
	assert_eq!(after.capacity(), after.len());
	assert_eq!(
		UnverifiedRoot2::decode(&after).unwrap().encode().unwrap(),
		before
	);
}
// Fills the permission section to `count` records alongside the twin's admitting permission
// (tenant 1), which stays; the added records use tenants 2 and up.
fn permissions(r: &mut Root2, count: usize) {
	let base = r.state.permissions.len();
	let a = r
		.state
		.authorities
		.values()
		.find(|a| a.revoked.is_none())
		.unwrap()
		.authority;
	for i in base..count.max(base) {
		let key = PermissionKey2 {
			authority: a,
			namespace: AdmissionNamespace2 {
				generation: r.state.generation,
				tenant: i as u64 + 1 + base as u64,
				scope: MutationScope::Raw,
				table: [0; 16],
				schema: 0,
			},
		};
		let rec = PermissionRecord2 {
			key,
			enrolled: l(4_100_000 + 2 * i as u64),
			revoked: if i % 2 == 0 {
				Some(l(4_100_001 + 2 * i as u64))
			} else {
				None
			},
		};
		r.state
			.permissions
			.insert(encode_permission_key2(&key).unwrap(), rec);
	}
	validate_state(&r.state).unwrap();
}
#[test]
fn exact_snapshot_matches_current_wire_for_minimal_and_bulk_models() {
	for name in ["E", "P1", "P256", "P4096", "X-small"] {
		equality(&model(name));
	}
	let v: Value = serde_json::from_str(include_str!("../root_profile_vectors.json")).unwrap();
	for item in v["refused"].as_array().unwrap() {
		let bytes = fixture(item["name"].as_str().unwrap());
		let u = UnverifiedRoot2::decode(&bytes).unwrap();
		let (g, c, l, fp) = u.fixture_fields();
		equality(&u.fixture_install(g, c, l, fp, hash(b"", &bytes)).unwrap());
	}
}
#[test]
fn exact_snapshot_matches_positive_permission_and_mixed_states() {
	// Fixed-width counts never change width at 127/128 or 255/256; test those values explicitly,
	// plus the minimal section (counts 0 and 1 both leave only the twin's admitting permission)
	// and the supported permission-count limit.
	for count in [0, 1, 2, 127, 128, 255, 256, 4096] {
		let mut r = model("E");
		permissions(&mut r, count);
		equality(&r);
	}
	for name in ["P1", "P256", "X-small"] {
		let mut r = model(name);
		permissions(&mut r, 2);
		equality(&r);
	}
}
#[test]
fn exact_snapshot_preserves_multi_fault_refusal_precedence() {
	let mut r = model("P1");
	permissions(&mut r, 2);
	for fault in 0..5 {
		let mut s = r.state.clone();
		match fault {
			0 => {
				s.generation = [0; 16];
				s.cluster = [0; 16];
				s.covered = l(1);
			}
			1 => {
				s.covered = l(1);
				s.initial_epoch = 0;
			}
			2 => {
				s.initial_epoch = 0;
				s.cluster = [0; 16];
			}
			3 => {
				s.covered = l(2);
			}
			_ => {
				s.command_digest = [0; 32];
				s.control_len = 240;
				s.cluster = [0; 16];
			}
		}
		let reference = encode_state_reference(&s).unwrap_err();
		let candidate = Root2 {
			state: s,
			limits: Limits2::default(),
		}
		.encode()
		.unwrap_err();
		assert_eq!(candidate, reference);
	}
}
