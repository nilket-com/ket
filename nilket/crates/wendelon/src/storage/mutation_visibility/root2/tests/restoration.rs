use super::*;
#[test]
fn s1_snapshot_plus_tail_matches_uninterrupted_reduction_and_old_view() {
	let r = root("root_reserved");
	let b = r.encode().unwrap();
	let u = UnverifiedRoot2::decode(&b).unwrap();
	assert_eq!(
		UnverifiedRoot2::decode(&b)
			.unwrap()
			.fixture_install(
				[9; 16],
				r.state.cluster,
				r.covered(),
				r.state.fingerprint,
				hash(b"", &b)
			)
			.err(),
		Some(RootError2::TrustMismatch)
	);
	let restored = u
		.fixture_install(
			r.state.generation,
			r.state.cluster,
			r.covered(),
			r.state.fingerprint,
			hash(b"", &b),
		)
		.unwrap();
	let c = finalized(&r);
	let (a, _) = next(&r, &c, &[], 1);
	let (b, _) = next(&restored, &c, &[], 1);
	assert_eq!(a.encode().unwrap(), b.encode().unwrap());
	let v = TrustedView2::fixture(a.state.generation, a.state.cluster, a.state.bootstrap, 6);
	assert!(
		a.lookup_at(&claim().manifest.identity, &v)
			.unwrap()
			.unwrap()
			.terminal
			.is_none()
	);
}
#[test]
fn s1_exact_last_entry_replay_and_changed_term_node_fingerprint_gap_refuse() {
	let r = root("root_reserved");
	let c = finalized(&r);
	let (g, cl) = (r.state.generation, r.state.cluster);
	let e = TrustedEntry2::fixture(g, cl, l(7), &c).unwrap();
	let v = VerifiedCommand2::fixture(g, cl, &c, &[], 1).unwrap();
	let (n, _) = super::s1c::checked_step(&r, &e, &v, &c).unwrap();
	let (n2, o) = super::s1c::checked_step(&n, &e, &v, &c).unwrap();
	assert!(matches!(o, ApplyResult2::AlreadyProcessed));
	assert_eq!(n2.encode().unwrap(), n.encode().unwrap());
	let mut altered = TrustedEntry2::fixture(g, cl, l(7), &c).unwrap();
	altered.change_fingerprint();
	assert_eq!(
		super::s1c::checked_step(&n, &altered, &v, &c).err(),
		Some(RootError2::TrustMismatch)
	);
	for (id, error) in [
		(
			Log2 {
				term: 2,
				node: 1,
				index: 7,
			},
			RootError2::TrustMismatch,
		),
		(
			Log2 {
				term: 1,
				node: 2,
				index: 7,
			},
			RootError2::TrustMismatch,
		),
		(l(9), RootError2::LineageGap),
		(l(5), RootError2::LineageGap),
	] {
		let e = TrustedEntry2::fixture(g, cl, id, &c).unwrap();
		assert_eq!(
			super::s1c::checked_step(&n, &e, &v, &c).err(),
			Some(error),
			"{id:?}"
		);
	}
}
#[test]
fn s1_serial_seeded_replay_keeps_held_bytes_constant() {
	let mut r = root("root_reserved");
	let baseline = r.usage().unwrap();
	let mut seed = 626u64;
	for _ in 0..128 {
		seed ^= seed << 13;
		seed ^= seed >> 7;
		seed ^= seed << 17;
		let c = if seed & 1 == 0 {
			Command2::Reserve(claim())
		} else {
			command("command_cover_blank")
		};
		let (n, _) = next(&r, &c, &[], 1);
		assert_eq!(n.usage().unwrap(), baseline);
		r = n;
	}
	let u = UnverifiedRoot2::decode(&r.encode().unwrap()).unwrap();
	assert_eq!(u.encode().unwrap(), r.encode().unwrap());
}
#[test]
fn s1_bootstrap_is_explicit_and_cannot_replace_an_existing_root() {
	let c = command("command_bootstrap_p3");
	let (g, cl) = ([0x11; 16], [0x12; 16]);
	let e = TrustedEntry2::fixture(g, cl, l(1), &c).unwrap();
	let v = VerifiedCommand2::fixture(g, cl, &c, &[], 1).unwrap();
	let (r, _) = Root2::reduce(None, &e, &v, &c).unwrap();
	assert_eq!(r.usage().unwrap().parents, 0);
	let (n, o) = next(&r, &c, &[], 1);
	assert_denied(&o, 1);
	assert_eq!(n.state.bootstrap, r.state.bootstrap);
}

#[test]
fn s1_checksum_valid_tampered_snapshot_is_not_trusted_by_prefix_only() {
	let r = root("root_reserved");
	let original = r.encode().unwrap();
	let mut changed = r.clone();
	changed.state.control_digest[0] ^= 1;
	let b = changed.encode().unwrap();
	let u = UnverifiedRoot2::decode(&b).unwrap();
	assert_eq!(
		u.fixture_install(
			r.state.generation,
			r.state.cluster,
			r.covered(),
			r.state.fingerprint,
			hash(b"", &original)
		)
		.err(),
		Some(RootError2::TrustMismatch)
	);
}
