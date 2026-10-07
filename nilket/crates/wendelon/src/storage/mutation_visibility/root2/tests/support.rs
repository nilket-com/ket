use super::*;

pub(super) fn fixture(name: &str) -> Vec<u8> {
	let j: serde_json::Value = serde_json::from_str(include_str!("goldens.json")).unwrap();
	let hex = j["fixtures"]
		.as_array()
		.unwrap()
		.iter()
		.find(|x| x["name"] == name)
		.unwrap()["hex"]
		.as_str()
		.unwrap();
	(0..hex.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
		.collect()
}
pub(super) fn command(name: &str) -> Command2 {
	Command2::decode(&fixture(name)).unwrap().2
}
pub(super) fn claim() -> ClaimProjection2 {
	ClaimProjection2::decode(&fixture("claim_projection2")).unwrap()
}
pub(super) fn original() -> OriginalGrant2 {
	OriginalGrant2::decode(&fixture("original_grant2")).unwrap()
}
pub(super) fn attestation() -> HolderAttestation2 {
	HolderAttestation2::decode(&fixture("holder_attestation1")).unwrap()
}
pub(super) fn l(i: u64) -> Log2 {
	Log2 {
		term: 1,
		node: 1,
		index: i,
	}
}
pub(super) fn root(name: &str) -> Root2 {
	let b = fixture(name);
	let u = UnverifiedRoot2::decode(&b).unwrap();
	let tuple = u.fixture_fields();
	u.fixture_install(tuple.0, tuple.1, tuple.2, tuple.3, hash(b"", &b))
		.unwrap()
}
pub(super) fn next(
	root: &Root2,
	c: &Command2,
	trace: &[OriginalGrant2],
	evidence: u8,
) -> (Root2, ApplyResult2) {
	let g = root.state.generation;
	let cluster = root.state.cluster;
	let e = TrustedEntry2::fixture(g, cluster, l(root.covered().index + 1), c).unwrap();
	let v = VerifiedCommand2::fixture(g, cluster, c, trace, evidence).unwrap();
	super::s1c::checked_step(root, &e, &v, c).unwrap()
}
pub(super) fn finalized(r: &Root2) -> Command2 {
	let p = r.lookup(&claim().manifest.identity).unwrap().unwrap();
	let mut a = attestation();
	a.reservation = p.reservation;
	Command2::Finalize {
		identity: p.claim.manifest.identity,
		digest: p.claim.digest().unwrap(),
		reservation: p.reservation,
		attestation: a,
	}
}
pub(super) fn closure(r: &Root2, cause: u8, anchor: Log2) -> Command2 {
	let p = r.lookup(&claim().manifest.identity).unwrap().unwrap();
	Command2::Resolve {
		identity: p.claim.manifest.identity,
		digest: p.claim.digest().unwrap(),
		reservation: p.reservation,
		cause,
		anchor,
	}
}
pub(super) fn assert_denied(o: &ApplyResult2, n: u16) {
	assert!(
		matches!(o,ApplyResult2::Denied{reason,..} if *reason==n),
		"{o:?}"
	)
}
