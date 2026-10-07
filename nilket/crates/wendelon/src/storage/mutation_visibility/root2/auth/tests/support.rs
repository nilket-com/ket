use super::*;
pub(super) fn bytes(name: &str) -> Vec<u8> {
	let values: serde_json::Value = serde_json::from_str(include_str!("vectors.json")).unwrap();
	hex(values[name].as_str().unwrap())
}
pub(super) fn hex(s: &str) -> Vec<u8> {
	assert_eq!(s.len() % 2, 0);
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
pub(super) fn claim() -> ClaimProjection2 {
	ClaimProjection2::decode(&bytes("claim_c2")).unwrap()
}
pub(super) fn holder() -> HolderAttestation2 {
	HolderAttestation2::decode(&bytes("holder_signed")).unwrap()
}
pub(super) fn cluster() -> [u8; 16] {
	bytes("cluster").try_into().unwrap()
}
pub(super) fn public_key() -> [u8; 32] {
	bytes("public_key").try_into().unwrap()
}
