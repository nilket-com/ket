use super::*;
#[test]
fn unknown_class_refuses_parentless_encode_and_decode() {
	let original = root("root_bootstrap_only");
	assert_eq!(original.state.parents.len(), 0);
	for n in [1, 136, 138, 188, 190, 240, 242, u64::MAX] {
		let mut r = original.clone();
		r.state.control_len = n;
		assert_eq!(r.encode().err(), Some(RootError2::ControlClass));
		let mut b = original.encode().unwrap();
		b[192..200].copy_from_slice(&n.to_le_bytes());
		let end = b.len() - 32;
		let sum = hash(b"", &b[..end]);
		b[end..].copy_from_slice(&sum);
		assert_eq!(
			UnverifiedRoot2::decode(&b).err(),
			Some(RootError2::ControlClass)
		);
	}
}
#[test]
fn bootstrap_classification_preserves_zero_and_refuses_unknown() {
	let r = root("root_bootstrap_only");
	let (g, c) = (r.state.generation, r.state.cluster);
	for n in [0, 137, 189, 241, 1, 136, u64::MAX] {
		let cmd = Command2::Bootstrap {
			epoch: 1,
			control: [7; 32],
			length: n,
		};
		let e = TrustedEntry2::fixture(g, c, l(1), &cmd).unwrap();
		let v = VerifiedCommand2::fixture(g, c, &cmd, &[], 0).unwrap();
		let got = Root2::reduce(None, &e, &v, &cmd);
		if matches!(n, 0 | 137 | 189 | 241) {
			assert_eq!(got.unwrap().0.state.control_len, n);
		} else {
			assert_eq!(got.err(), Some(RootError2::ControlClass));
		}
	}
}
