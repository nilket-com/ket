use super::*;

#[test]
fn identity_fixed_bytes_and_caller_hints_are_separate() {
	let id = identity(0x0807060504030201);
	let bytes = id.encode().unwrap();
	assert_eq!(bytes.len(), 73);
	assert_eq!(&bytes[..16], &[1; 16]);
	assert_eq!(&bytes[16..24], &[7, 0, 0, 0, 0, 0, 0, 0]);
	assert_eq!(bytes[24], 1);
	assert_eq!(&bytes[25..41], &[2; 16]);
	assert_eq!(&bytes[41..49], &[3, 0, 0, 0, 0, 0, 0, 0]);
	assert_eq!(&bytes[49..65], &[4; 16]);
	assert_eq!(&bytes[65..], &[1, 2, 3, 4, 5, 6, 7, 8]);
	assert_eq!(MutationIdentity::decode(&bytes).unwrap(), id);
	let caller = crate::schema::core::ClientWriteId {
		client_uuid: id.caller,
		sequence_no: id.sequence,
		idempotency_ttl_ms: 1000,
		_pad: 0,
	};
	let hints = crate::schema::core::ClientWriteId {
		idempotency_ttl_ms: 300000,
		_pad: 99,
		..caller
	};
	assert_eq!(
		caller.canonical_idempotency_bytes(),
		hints.canonical_idempotency_bytes()
	);
	assert_eq!(caller.canonical_idempotency_bytes(), id.caller_bytes());
}
#[test]
fn identity_rejects_malformed_scope_zero_and_trailing() {
	for offset in [0usize, 25, 49] {
		let mut b = identity(1).encode().unwrap();
		b[offset..offset + 16].fill(0);
		assert!(MutationIdentity::decode(&b).is_err());
	}
	let mut b = identity(1).encode().unwrap();
	b[24] = 9;
	assert!(MutationIdentity::decode(&b).is_err());
	b[24] = 0;
	assert!(MutationIdentity::decode(&b).is_err());
	let mut raw = identity(1);
	raw.scope = MutationScope::Raw;
	raw.table = [0; 16];
	raw.schema_generation = 0;
	assert_eq!(
		MutationIdentity::decode(&raw.encode().unwrap()).unwrap(),
		raw
	);
	let mut b = raw.encode().unwrap().to_vec();
	b.push(0);
	assert_eq!(
		MutationIdentity::decode(&b),
		Err(VisibilityError::TrailingBytes)
	);
	assert!(
		Admission {
			epoch: 0,
			holder: 1
		}
		.encode()
		.is_err()
	);
	assert!(
		Admission {
			epoch: 1,
			holder: 0
		}
		.encode()
		.is_err()
	);
	assert!(
		ProofKeyBinding {
			key_version: 0,
			..manifest(1).binding()
		}
		.encode()
		.is_err()
	);
	assert!(DecisionLogId { node: 0, ..log(1) }.encode().is_err());
}
#[test]
fn one_row_manifest_exact_golden_layout_and_sizes() {
	let m = manifest(9);
	let b = m.encode().unwrap();
	assert_eq!(b.len(), 212);
	assert_eq!(&b[..2], &[1, 0]);
	assert_eq!(&b[2..75], &identity(9).encode().unwrap());
	assert_eq!(&b[75..87], &[1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0]);
	assert_eq!(&b[87..103], &[5; 16]);
	assert_eq!(&b[103..112], &[1, 0, 0, 0, 1, 1, 0, 1, 0]);
	assert_eq!(
		&b[112..143],
		&[
			0, 0, 0, 0, 10, 0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0,
			0, 1, 0
		]
	);
	assert_eq!(&b[143..147], &[0; 4]);
	assert_eq!(&b[147..179], &[6; 32]);
	assert_eq!(&b[179..211], &[7; 32]);
	assert_eq!(b[211], 0);
	assert_eq!(ParentManifest::decode(&b).unwrap(), m);
	let r = record(
		authority()
			.apply(&event(2, 9, TerminalDecision::Accepted))
			.unwrap(),
	);
	assert_eq!(r.encode().unwrap().len(), 270);
	assert_eq!(r.stored_bytes().unwrap(), 274);
	assert_eq!(274 * MAX_DECISIONS, 8978432);
}
fn many(children: usize, rows: usize) -> ParentManifest {
	let mut m = manifest(1);
	m.children.clear();
	let mut token = 0u64;
	for c in 0..children {
		let n = if c == 0 { rows - children + 1 } else { 1 };
		let mut versions = Vec::new();
		for r in 0..n {
			token += 1;
			let mut key = [0; 32];
			key[..8].copy_from_slice(&token.to_le_bytes());
			versions.push(LogicalVersion {
				ordinal: r as u32,
				key_token: key,
				content_commitment: [7; 32],
				tombstone: false,
			});
		}
		m.children.push(ManifestChild {
			ordinal: c as u32,
			range_start: 0,
			range_end: u64::MAX,
			partition: None,
			versions,
		});
	}
	m
}
#[test]
fn manifest_limit_boundaries_and_unknown_tags_refuse() {
	let m = many(256, 512);
	assert_eq!(m.encode().unwrap().len(), 43376);
	assert_eq!(ParentManifest::decode(&m.encode().unwrap()).unwrap(), m);
	for m in [many(257, 512), many(1, 513)] {
		assert_eq!(m.encode(), Err(VisibilityError::LimitExceeded));
	}
	let m = manifest(1);
	let b = m.encode().unwrap();
	for (offset, value) in [(0, 2), (107, 99), (108, 2), (132, 2), (211, 2)] {
		let mut forged = b.clone();
		forged[offset] = value;
		assert!(ParentManifest::decode(&forged).is_err(), "offset {offset}");
	}
	let mut huge = b.clone();
	huge[110..112].copy_from_slice(&u16::MAX.to_le_bytes());
	assert_eq!(
		ParentManifest::decode(&huge),
		Err(VisibilityError::LimitExceeded)
	);
	let mut huge = b.clone();
	huge[141..143].copy_from_slice(&u16::MAX.to_le_bytes());
	assert_eq!(
		ParentManifest::decode(&huge),
		Err(VisibilityError::LimitExceeded)
	);
	for n in 0..b.len() {
		assert!(ParentManifest::decode(&b[..n]).is_err());
	}
	let mut padded = b;
	padded.push(0);
	assert_eq!(
		ParentManifest::decode(&padded),
		Err(VisibilityError::TrailingBytes)
	);
	assert_eq!(
		ParentManifest::decode(&vec![0; MAX_MANIFEST_BYTES + 1]),
		Err(VisibilityError::LimitExceeded)
	);
}
#[test]
fn manifest_ambiguity_and_scope_errors_are_not_silently_sorted() {
	let mut m = many(2, 2);
	m.children[1].versions[0].key_token = m.children[0].versions[0].key_token;
	assert!(m.encode().is_err());
	let mut m = manifest(1);
	m.children[0].ordinal = 1;
	assert!(m.encode().is_err());
	m.children[0].ordinal = 0;
	m.children[0].versions[0].ordinal = 1;
	assert!(m.encode().is_err());
	for mutate in [0, 1, 2, 3] {
		let mut m = manifest(1);
		match mutate {
			0 => m.children[0].range_start = 101,
			1 => m.children[0].partition = Some(0),
			2 => m.children[0].versions[0].tombstone = true,
			_ => m.operation = MutationOperation::RawPut,
		};
		assert!(m.encode().is_err());
	}
	let mut m = manifest(1);
	m.operation = MutationOperation::Delete;
	m.children[0].versions[0].tombstone = true;
	assert!(m.encode().is_ok());
}
