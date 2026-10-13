use super::*;
#[test]
fn exact_envelope_size_preserves_limit_then_overflow() {
	for body in [0, 1, 127, 128, 255, 256, 4096] {
		let old = envelope_versioned(b"VISAUT04", 4, &vec![0; body], 4096).unwrap();
		assert_eq!(envelope_size(body, 4096).unwrap(), (body as u32, old.len()));
	}
	assert_eq!(envelope_size(4097, 4096), Err(RootError2::Limit));
	assert_eq!(
		envelope_versioned(b"VISAUT04", 4, &vec![0; 4097], 4096),
		Err(RootError2::Limit)
	);
	assert_eq!(
		envelope_size(usize::MAX, ROOT_BYTES2),
		Err(RootError2::Limit)
	);
	// Real states are capped at160MiB before sizing. These widths cannot be reached
	// by valid states; exercise arithmetic without allocating multi-GiB inputs.
	assert_eq!(
		envelope_size(usize::MAX, usize::MAX),
		Err(RootError2::Overflow)
	);
}

use crate::storage::mutation_visibility::MutationScope;
use crate::storage::mutation_visibility::root2::admin_statement::{
	AdmissionNamespace2, PermissionKey2,
};
use crate::storage::mutation_visibility::root2::admission_registry::{
	PermissionRecord2, encode_permission_section2,
};
#[test]
fn streamed_permissions_preserve_section_bytes_and_refusals() {
	let mut permissions = PermissionMap2::new();
	for tenant in [1, 2] {
		let key = PermissionKey2 {
			authority: AuthorityRef2 {
				id: [1; 16],
				revision: 1,
			},
			namespace: AdmissionNamespace2 {
				generation: [2; 16],
				tenant,
				scope: MutationScope::Raw,
				table: [0; 16],
				schema: 0,
			},
		};
		permissions.insert(
			encode_permission_key2(&key).unwrap(),
			PermissionRecord2 {
				key,
				enrolled: Log2 {
					term: 1,
					node: 1,
					index: tenant,
				},
				revoked: None,
			},
		);
	}
	for limits in [
		permission_limits2(),
		PermissionLimits2 {
			count: 1,
			..permission_limits2()
		},
		PermissionLimits2 {
			section: 4 + 117,
			..permission_limits2()
		},
		PermissionLimits2 {
			count: PERMISSIONS2 + 1,
			..permission_limits2()
		},
	] {
		for r in permissions.values() {
			assert_eq!(
				encode_permission_record2(r).unwrap().len(),
				PERMISSION_RECORD_BYTES2
			);
		}
		let old = encode_permission_section2(&permissions, limits);
		let mut b = Vec::new();
		let new = visit_permission_section2(&permissions, limits, &mut |x| b.extend_from_slice(x))
			.map(|()| b);
		assert_eq!(new, old);
	}
	let rec = *permissions.values().next().unwrap();
	let mut bad = PermissionMap2::new();
	bad.insert([0; 73], rec);
	let mut b = Vec::new();
	assert_eq!(
		visit_permission_section2(&bad, permission_limits2(), &mut |x| b.extend_from_slice(x)),
		encode_permission_section2(&bad, permission_limits2()).map(|_| ())
	);
}
