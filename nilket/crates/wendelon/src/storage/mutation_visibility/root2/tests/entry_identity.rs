use crate::consensus::WendelonRequest;
use crate::storage::mutation_visibility::root2::entry_identity::*;
use crate::storage::mutation_visibility::root2::types::Digest2;
use aws_lc_rs::digest::{SHA256, digest};
use openraft::{BasicNode, Membership};
use std::collections::{BTreeMap, BTreeSet};

fn vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("identity_vectors.json")).unwrap()
}
fn case(name: &str) -> serde_json::Value {
	vectors()["cases"]
		.as_array()
		.unwrap()
		.iter()
		.find(|c| c["name"] == name)
		.unwrap()
		.clone()
}
fn unhex(s: &str) -> Vec<u8> {
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn at(c: &serde_json::Value) -> EntryPosition2 {
	EntryPosition2 {
		term: c["term"].as_u64().unwrap(),
		node: c["node"].as_u64().unwrap() as u32,
		index: c["index"].as_u64().unwrap(),
	}
}
fn expected(c: &serde_json::Value) -> Digest2 {
	unhex(c["digest"].as_str().unwrap()).try_into().unwrap()
}
// Allocated reference preimage, built independently of the streaming implementation.
fn reference(c: &serde_json::Value, kind: u8, payload: &[u8]) -> Digest2 {
	let p = at(c);
	let mut pre = b"VISROOT2-ENTRY\0".to_vec();
	pre.extend(1u16.to_le_bytes());
	pre.extend(p.term.to_le_bytes());
	pre.extend(p.node.to_le_bytes());
	pre.extend(p.index.to_le_bytes());
	pre.push(kind);
	pre.extend((payload.len() as u64).to_le_bytes());
	pre.extend(payload);
	if let Some(h) = c["preimage"].as_str() {
		assert_eq!(pre, unhex(h), "reference preimage");
	}
	digest(&SHA256, &pre).as_ref().try_into().unwrap()
}
fn membership() -> Membership<u32, BasicNode> {
	Membership::new(
		vec![BTreeSet::from([1u32])],
		BTreeMap::from([(1u32, BasicNode { addr: "n1".into() })]),
	)
}
#[test]
fn genesis_membership_and_blank_match_independent_vectors() {
	let g = case("genesis_membership");
	let m = membership();
	let bytes = postcard::to_allocvec(&m).unwrap();
	assert_eq!(bytes, unhex(g["payload"].as_str().unwrap()));
	assert_eq!(membership_entry_identity2(at(&g), &m), Ok(expected(&g)));
	assert_eq!(reference(&g, 2, &bytes), expected(&g));
	let b = case("blank");
	assert_eq!(blank_entry_identity2(at(&b)), expected(&b));
	assert_eq!(reference(&b, 0, &[]), expected(&b));
}
#[test]
fn normal_entry_matches_independent_vector_and_allocated_reference() {
	let c = case("normal_schema_update_small");
	let r = WendelonRequest::SchemaUpdate(vec![0xab; 3]);
	let bytes = postcard::to_allocvec(&r).unwrap();
	assert_eq!(bytes, unhex(c["payload"].as_str().unwrap()));
	assert_eq!(normal_entry_identity2(at(&c), &r), Ok(expected(&c)));
	assert_eq!(reference(&c, 1, &bytes), expected(&c));
}
#[test]
fn a_value_over_five_mebibytes_is_identified_without_a_size_cap() {
	let c = case("normal_schema_update_6mib");
	assert_eq!(c["payload_len"].as_u64(), Some(6 * 1024 * 1024 + 5));
	let r = WendelonRequest::SchemaUpdate(vec![7; 6 * 1024 * 1024]);
	assert_eq!(normal_entry_identity2(at(&c), &r), Ok(expected(&c)));
	assert_eq!(
		reference(&c, 1, &postcard::to_allocvec(&r).unwrap()),
		expected(&c)
	);
}
#[test]
fn kind_position_and_payload_each_change_the_identity() {
	let base = EntryPosition2 {
		term: 1,
		node: 3,
		index: 2,
	};
	let r = WendelonRequest::SchemaUpdate(vec![1]);
	let id = normal_entry_identity2(base, &r).unwrap();
	for p in [
		EntryPosition2 { term: 2, ..base },
		EntryPosition2 { node: 4, ..base },
		EntryPosition2 { index: 3, ..base },
	] {
		assert_ne!(normal_entry_identity2(p, &r).unwrap(), id);
	}
	assert_ne!(
		normal_entry_identity2(base, &WendelonRequest::SchemaUpdate(vec![2])).unwrap(),
		id
	);
	assert_ne!(blank_entry_identity2(base), id);
}
#[test]
fn running_count_overflow_and_count_mismatch_are_errors_not_panics() {
	assert_eq!(stream_probe(5, &[1, 2, 3]), Ok(8));
	assert_eq!(
		stream_probe(u64::MAX - 1, &[1, 2]),
		Err(EntryIdentityError2::Overflow)
	);
	assert_eq!(stream_probe(u64::MAX, &[]), Ok(u64::MAX));
	assert!(finish_probe(4, 4).is_ok());
	assert_eq!(finish_probe(3, 4), Err(EntryIdentityError2::CountMismatch));
	assert_eq!(finish_probe(5, 4), Err(EntryIdentityError2::CountMismatch));
}
