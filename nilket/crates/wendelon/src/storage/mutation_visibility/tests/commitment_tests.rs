use super::*;
use std::cell::RefCell;

struct Capture {
	binding: ProofKeyBinding,
	bytes: RefCell<Vec<u8>>,
}
impl ProofKeyProvider for Capture {
	fn keyed_digest(
		&self,
		binding: &ProofKeyBinding,
		bytes: &[u8],
	) -> Result<[u8; 32], ProofKeyUnavailable> {
		assert_eq!(binding, &self.binding);
		*self.bytes.borrow_mut() = bytes.to_vec();
		Ok([9; 32])
	}
}
fn context(id: MutationIdentity, token: [u8; 32]) -> RowCommitmentContext {
	RowCommitmentContext {
		identity: id,
		admission: Admission {
			epoch: 1,
			holder: 1,
		},
		operation: MutationOperation::Insert,
		child_ordinal: 0,
		row_ordinal: 0,
		key_token: token,
		tombstone: false,
	}
}
#[test]
fn exact_domain_preimages_and_synthetic_commitment_goldens() {
	let m = manifest(1);
	let binding = m.binding();
	let p = Capture {
		binding,
		bytes: RefCell::new(Vec::new()),
	};
	let key = b"customer@example.com";
	key_token(
		&p,
		&binding,
		&m.identity,
		CanonicalKeyBytes::new(key).unwrap(),
	)
	.unwrap();
	let mut expected = b"wendelon/adr0100/key-token/v1\0".to_vec();
	expected.extend_from_slice(&binding.encode().unwrap());
	expected.extend_from_slice(&m.identity.encode().unwrap()[..41]);
	expected.extend_from_slice(&[1, 0]);
	expected.extend_from_slice(&20u32.to_le_bytes());
	expected.extend_from_slice(key);
	assert_eq!(*p.bytes.borrow(), expected);
	let c = context(m.identity, [6; 32]);
	row_content_commitment(
		&p,
		&binding,
		&c,
		CanonicalContentBytes::new(b"row").unwrap(),
	)
	.unwrap();
	let mut expected = b"wendelon/adr0100/row-content/v1\0".to_vec();
	expected.extend_from_slice(&binding.encode().unwrap());
	expected.extend_from_slice(&m.identity.encode().unwrap());
	expected.extend_from_slice(&[1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0]);
	expected.push(1);
	expected.extend_from_slice(&[0; 8]);
	expected.extend_from_slice(&[6; 32]);
	expected.extend_from_slice(&[0, 1, 0]);
	expected.extend_from_slice(&3u32.to_le_bytes());
	expected.extend_from_slice(b"row");
	assert_eq!(*p.bytes.borrow(), expected);
	parent_manifest_commitment(&p, &m).unwrap();
	let mut expected = b"wendelon/adr0100/parent-manifest/v1\0".to_vec();
	expected.extend_from_slice(&binding.encode().unwrap());
	expected.extend_from_slice(&212u32.to_le_bytes());
	expected.extend_from_slice(&m.encode().unwrap());
	assert_eq!(*p.bytes.borrow(), expected);
	let real = TestProofKey::new(binding, [0x99; 32]);
	let k = key_token(
		&real,
		&binding,
		&m.identity,
		CanonicalKeyBytes::new(key).unwrap(),
	)
	.unwrap();
	let r = row_content_commitment(
		&real,
		&binding,
		&c,
		CanonicalContentBytes::new(b"row").unwrap(),
	)
	.unwrap();
	let parent = parent_manifest_commitment(&real, &m).unwrap();
	println!(
		"GOLDEN key={} row={} parent={}",
		hex(&k),
		hex(&r),
		hex(&parent)
	);
	assert_eq!(
		hex(&k),
		"7f311f1e1ccae45ef34e4b96042c906f29db57633cc6d8289885a81ba3348096"
	);
	assert_eq!(
		hex(&r),
		"dc0dd7267a4e196a9746a19a150cf3a7e5ecbbfdc78f57e2175839d79ade2e30"
	);
	assert_eq!(
		hex(&parent),
		"7b0adf9de723eee3373850f36f88bef2c61e0021d2dd1d645b6dc9df6c6535b3"
	);
	// Exact preimage assertions above are independent of the digest-provider implementation.
	assert_ne!(k, r);
	assert_ne!(r, parent);
}
#[test]
fn schema_bump_preserves_key_token_but_not_identity_or_content() {
	let m = manifest(1);
	let p = TestProofKey::new(m.binding(), [0x99; 32]);
	let key = CanonicalKeyBytes::new(b"low-entropy").unwrap();
	let a = key_token(&p, &m.binding(), &m.identity, key).unwrap();
	let mut changed = m.clone();
	changed.identity.schema_generation += 1;
	let b = key_token(
		&p,
		&changed.binding(),
		&changed.identity,
		CanonicalKeyBytes::new(b"low-entropy").unwrap(),
	)
	.unwrap();
	assert_eq!(a, b);
	assert_ne!(
		m.identity.encode().unwrap(),
		changed.identity.encode().unwrap()
	);
	assert_ne!(
		row_content_commitment(
			&p,
			&m.binding(),
			&context(m.identity, a),
			CanonicalContentBytes::new(b"same row").unwrap()
		)
		.unwrap(),
		row_content_commitment(
			&p,
			&changed.binding(),
			&context(changed.identity, b),
			CanonicalContentBytes::new(b"same row").unwrap()
		)
		.unwrap()
	);
	assert_ne!(
		parent_manifest_commitment(&p, &m).unwrap(),
		parent_manifest_commitment(&p, &changed).unwrap()
	);
}
#[test]
fn key_contexts_domains_and_version_do_not_substitute() {
	let m = manifest(1);
	let p = TestProofKey::new(m.binding(), [0x99; 32]);
	let original = key_token(
		&p,
		&m.binding(),
		&m.identity,
		CanonicalKeyBytes::new(b"key").unwrap(),
	)
	.unwrap();
	for field in 0..4 {
		let mut id = m.identity;
		let mut binding = m.binding();
		match field {
			0 => {
				id.tenant += 1;
				binding.tenant += 1
			}
			1 => {
				id.generation = [8; 16];
				binding.generation = [8; 16]
			}
			2 => id.table = [8; 16],
			_ => binding.key_version = 2,
		};
		let pp = TestProofKey::new(binding, [0x99; 32]);
		assert_ne!(
			key_token(&pp, &binding, &id, CanonicalKeyBytes::new(b"key").unwrap()).unwrap(),
			original
		);
	}
	let c = context(m.identity, original);
	let base = row_content_commitment(
		&p,
		&m.binding(),
		&c,
		CanonicalContentBytes::new(b"row").unwrap(),
	)
	.unwrap();
	for field in 0..6 {
		let mut ctx = c;
		match field {
			0 => ctx.identity.sequence += 1,
			1 => ctx.child_ordinal = 1,
			2 => ctx.row_ordinal = 1,
			3 => ctx.admission.epoch += 1,
			4 => ctx.operation = MutationOperation::Update,
			_ => ctx.key_token = [8; 32],
		};
		assert_ne!(
			row_content_commitment(
				&p,
				&m.binding(),
				&ctx,
				CanonicalContentBytes::new(b"row").unwrap()
			)
			.unwrap(),
			base
		);
	}
	assert_ne!(
		row_content_commitment(
			&p,
			&m.binding(),
			&c,
			CanonicalContentBytes::new(b"changed").unwrap()
		)
		.unwrap(),
		base
	);
}
#[test]
fn tombstones_inputs_and_provider_failures_are_explicit() {
	let m = manifest(1);
	let p = TestProofKey::new(m.binding(), [0x99; 32]);
	let mut c = context(m.identity, [6; 32]);
	c.operation = MutationOperation::Delete;
	c.tombstone = true;
	assert!(
		row_content_commitment(
			&p,
			&m.binding(),
			&c,
			CanonicalContentBytes::new(b"nonempty").unwrap()
		)
		.is_err()
	);
	assert!(
		row_content_commitment(
			&p,
			&m.binding(),
			&c,
			CanonicalContentBytes::new(b"").unwrap()
		)
		.is_ok()
	);
	assert!(CanonicalKeyBytes::new(b"").is_err());
	assert!(CanonicalKeyBytes::new(&vec![0; MAX_KEY_INPUT]).is_ok());
	assert!(CanonicalKeyBytes::new(&vec![0; MAX_KEY_INPUT + 1]).is_err());
	assert!(CanonicalContentBytes::new(&vec![0; MAX_CONTENT_INPUT]).is_ok());
	assert!(CanonicalContentBytes::new(&vec![0; MAX_CONTENT_INPUT + 1]).is_err());
	let mut bad = m.binding();
	bad.tenant += 1;
	assert!(
		key_token(
			&p,
			&bad,
			&m.identity,
			CanonicalKeyBytes::new(b"key").unwrap()
		)
		.is_err()
	);
	let mut wrong_version = m.binding();
	wrong_version.key_version = 2;
	assert_eq!(
		key_token(
			&p,
			&wrong_version,
			&m.identity,
			CanonicalKeyBytes::new(b"key").unwrap()
		),
		Err(CommitmentError::Key(ProofKeyUnavailable::WrongVersion))
	);
	struct Missing(ProofKeyUnavailable);
	impl ProofKeyProvider for Missing {
		fn keyed_digest(
			&self,
			_: &ProofKeyBinding,
			_: &[u8],
		) -> Result<[u8; 32], ProofKeyUnavailable> {
			Err(self.0)
		}
	}
	for reason in [
		ProofKeyUnavailable::Unavailable,
		ProofKeyUnavailable::WrongScope,
		ProofKeyUnavailable::WrongVersion,
		ProofKeyUnavailable::NotAuthorized,
		ProofKeyUnavailable::BackendFailure,
	] {
		assert_eq!(
			parent_manifest_commitment(&Missing(reason), &m),
			Err(CommitmentError::Key(reason))
		);
	}
}
#[test]
fn restore_same_key_is_stable_and_debug_is_redacted() {
	let m = manifest(1);
	let a = TestProofKey::new(m.binding(), [0x99; 32]);
	let b = TestProofKey::new(m.binding(), [0x99; 32]);
	let wrong = TestProofKey::new(m.binding(), [0x88; 32]);
	let expected = parent_manifest_commitment(&a, &m).unwrap();
	assert_eq!(parent_manifest_commitment(&b, &m).unwrap(), expected);
	assert_ne!(parent_manifest_commitment(&wrong, &m).unwrap(), expected);
	let text = format!(
		"{:?} {:?} {:?} {:?} {:?}",
		m,
		m.identity,
		m.binding(),
		CanonicalKeyBytes::new(b"customer@example.com").unwrap(),
		context(m.identity, [6; 32])
	);
	assert!(!text.contains("customer"));
	assert!(!text.contains(&hex(&expected)));
	assert!(text.contains("redacted"));
}
