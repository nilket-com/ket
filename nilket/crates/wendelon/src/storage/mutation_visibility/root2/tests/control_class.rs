use super::*;
/// Every commitment length other than the profile-3 manifest's (241) is an unknown class: 0 (the
/// removed class 0), the removed profile-1/2 lengths and every other value up to 242, and the maximum.
fn refused() -> impl Iterator<Item = u64> {
	(0..=242).filter(|&n| n != 241).chain([u64::MAX])
}
#[test]
fn unknown_class_refuses_parentless_encode_and_decode() {
	let original = root("root_bootstrap_only");
	assert_eq!(original.state.parents.len(), 0);
	assert_eq!(original.state.control_len, 241);
	assert!(original.encode().is_ok());
	for n in refused() {
		let mut r = original.clone();
		r.state.control_len = n;
		assert_eq!(r.encode().err(), Some(RootError2::ControlClass), "{n}");
		let mut b = original.encode().unwrap();
		b[192..200].copy_from_slice(&n.to_le_bytes());
		let end = b.len() - 32;
		let sum = hash(b"", &b[..end]);
		b[end..].copy_from_slice(&sum);
		assert_eq!(
			UnverifiedRoot2::decode(&b).err(),
			Some(RootError2::ControlClass),
			"{n}"
		);
	}
}
#[test]
fn bootstrap_classification_accepts_only_241() {
	let r = root("root_bootstrap_only");
	let (g, c) = (r.state.generation, r.state.cluster);
	for n in refused().chain([241]) {
		let cmd = Command2::Bootstrap {
			epoch: 1,
			control: [7; 32],
			length: n,
		};
		let e = TrustedEntry2::fixture(g, c, l(1), &cmd).unwrap();
		let v = VerifiedCommand2::fixture(g, c, &cmd, &[], 0).unwrap();
		let got = Root2::reduce(None, &e, &v, &cmd);
		if n == 241 {
			assert_eq!(got.unwrap().0.state.control_len, n);
		} else {
			assert_eq!(got.err(), Some(RootError2::ControlClass), "{n}");
		}
	}
}
