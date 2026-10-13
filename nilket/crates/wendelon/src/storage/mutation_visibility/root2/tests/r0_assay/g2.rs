//! A3b direct construction; deliberately separate from replay-derived A3.
use super::*;
use crate::test_support::g2::{Phase, case, emit};
#[test]
#[ignore = "G2 A3b: isolated reviewed synthetic runner only"]
fn g2_bulk_snapshot() {
	let c = case();
	assert_eq!(c["assay"], "A3b");
	assert_eq!(c["op"], "encode");
	let name = c["direct_model"].as_str().unwrap();
	assert!(["E", "P256", "P4096", "X-small"].contains(&name));
	let root = model(name);
	let bytes = root.encode().unwrap();
	assert_eq!(&bytes[..8], b"VISAUT03");
	let u = root.usage().unwrap();
	emit(
		json!({"event":"fixture","case":c,"scope":"A3b direct construction, NOT replay-derived",
		"encoded_bytes":bytes.len(),"parents":u.parents,"held":u.held,"history":u.history,"registry":u.registry,
		"lineages":root.state.lineages.len(),"cuts":root.state.cuts.len(),"permissions":root.state.permissions.len(),
		"encoded_sha256":hex_digest(hash(b"",&bytes)),"baseline":"root and canonical expected bytes retained"}),
	);
	let p = Phase::begin("encode");
	let output = root.encode().unwrap();
	let metric = p.end();
	assert_eq!(output, bytes);
	emit(metric);
	std::hint::black_box(output);
}
