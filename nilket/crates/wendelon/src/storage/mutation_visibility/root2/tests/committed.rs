//! Stage A0: the dormant committed-entry adapter over real openraft entries. Every position,
//! identity and class comes from the entry; the source capability is the test-only mint.
use super::*;
use crate::consensus::raft_storage::body_format_corpus::canonical_requests;
use crate::consensus::{WendelonRequest, WendelonTypeConfig};
use crate::storage::mutation_visibility::root2::committed::*;
use crate::storage::mutation_visibility::root2::entry_identity::*;
use crate::storage::mutation_visibility::root2::origin::*;
use crate::storage::mutation_visibility::root2::trust::CommittedEntry2;
use openraft::{BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Membership};
use std::collections::{BTreeMap, BTreeSet};

fn vector(name: &str) -> serde_json::Value {
	let all: serde_json::Value =
		serde_json::from_str(include_str!("identity_vectors.json")).unwrap();
	all["cases"]
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
fn digest(name: &str) -> Digest2 {
	unhex(vector(name)["digest"].as_str().unwrap())
		.try_into()
		.unwrap()
}
fn log(term: u64, node: u32, index: u64) -> LogId<u32> {
	LogId::new(CommittedLeaderId::new(term, node), index)
}
fn entry(
	log_id: LogId<u32>,
	payload: EntryPayload<WendelonTypeConfig>,
) -> Entry<WendelonTypeConfig> {
	Entry { log_id, payload }
}
/// Genesis exactly as openraft's `initialize` builds it: `Membership::from(map)` at the default
/// log id.
fn genesis(nodes: BTreeMap<u32, BasicNode>) -> Entry<WendelonTypeConfig> {
	entry(
		LogId::default(),
		EntryPayload::Membership(Membership::from(nodes)),
	)
}
fn adapt(e: &Entry<WendelonTypeConfig>) -> CommittedEntry2 {
	committed_entry2(&CommittedSource2::synthetic(), e).unwrap()
}

#[test]
fn genesis_from_real_openraft_constructors() {
	let nodes = BTreeMap::from([(1u32, BasicNode { addr: "n1".into() })]);
	let g = genesis(nodes.clone());
	let (position, identity, class) = adapt(&g).entry_fields();
	assert!(
		position
			== EntryPosition2 {
				term: 0,
				node: 0,
				index: 0
			}
	);
	assert!(class == EntryClass2::Membership);
	let EntryPayload::Membership(real) = &g.payload else {
		unreachable!()
	};
	assert_eq!(membership_entry_identity2(position, real), Ok(identity));
	// The accepted vector was built with Membership::new over the same single voter and address.
	// It is compared, never rewritten: the real constructor's bytes must equal it.
	let pinned = vector("genesis_membership");
	let vector_membership = Membership::new(vec![BTreeSet::from([1u32])], nodes);
	let real_bytes = postcard::to_allocvec(real).unwrap();
	assert_eq!(
		postcard::to_allocvec(&vector_membership).unwrap(),
		real_bytes,
		"Membership::from(map) and Membership::new serialize identically"
	);
	assert_eq!(real_bytes, unhex(pinned["payload"].as_str().unwrap()));
	assert_eq!(identity, digest("genesis_membership"));
}

#[test]
fn blank_and_normal_match_vectors() {
	let blank = adapt(&entry(log(1, 3, 1), EntryPayload::Blank)).entry_fields();
	assert_eq!(blank.1, digest("blank"));
	assert!(blank.2 == EntryClass2::Blank);
	let small = adapt(&entry(
		log(1, 3, 2),
		EntryPayload::Normal(WendelonRequest::SchemaUpdate(vec![0xab; 3])),
	))
	.entry_fields();
	assert_eq!(small.1, digest("normal_schema_update_small"));
	assert!(small.2 == EntryClass2::NonRoot2);
	let large = adapt(&entry(
		log(2, 1, 900),
		EntryPayload::Normal(WendelonRequest::SchemaUpdate(vec![7; 6 * 1024 * 1024])),
	))
	.entry_fields();
	assert_eq!(large.1, digest("normal_schema_update_6mib"));
	assert!(
		large.0
			== EntryPosition2 {
				term: 2,
				node: 1,
				index: 900
			}
	);
}

#[test]
fn every_request_variant_is_non_root2() {
	let corpus = canonical_requests();
	let mut seen: Vec<&str> = Vec::new();
	for (i, (_, request)) in corpus.into_iter().enumerate() {
		let name = request.variant_name();
		let e = entry(log(1, 1, i as u64 + 1), EntryPayload::Normal(request));
		assert!(
			adapt(&e).entry_fields().2
				== if name == "root2_command" {
					EntryClass2::HeldCarrier
				} else {
					EntryClass2::NonRoot2
				},
			"{name}"
		);
		if !seen.contains(&name) {
			seen.push(name);
		}
	}
	seen.sort_unstable();
	let mut all = WendelonRequest::ALL_VARIANT_NAMES.to_vec();
	all.sort_unstable();
	assert_eq!(seen, all, "every variant exercised");
}

#[test]
fn adapter_feeds_the_pre_root_scan() {
	let g = genesis(BTreeMap::from([(1u32, BasicNode { addr: "n1".into() })]));
	let later = Membership::from(BTreeMap::from([
		(1u32, BasicNode { addr: "n1".into() }),
		(2u32, BasicNode { addr: "n2".into() }),
	]));
	let entries = [
		g.clone(),
		entry(log(1, 1, 1), EntryPayload::Blank),
		entry(
			log(1, 1, 2),
			EntryPayload::Normal(WendelonRequest::SchemaUpdate(vec![1])),
		),
		entry(log(1, 1, 3), EntryPayload::Membership(later)),
		entry(log(2, 1, 4), EntryPayload::Blank),
	];
	let mut state = empty_authority2();
	for (i, e) in entries.iter().enumerate() {
		let (next, step) = advance_pre_root2(&state, &adapt(e)).unwrap();
		let want = if i == 0 {
			PreRootStep2::Genesis
		} else {
			PreRootStep2::Scanned
		};
		assert_eq!(step, want, "entry {i}");
		state = next;
	}
	// Re-feeding the last entry is idempotent; skipping an index is a gap.
	let (_, step) = advance_pre_root2(&state, &adapt(&entries[4])).unwrap();
	assert_eq!(step, PreRootStep2::AlreadyProcessed);
	let skipped = entry(log(2, 1, 6), EntryPayload::Blank);
	assert_eq!(
		advance_pre_root2(&state, &adapt(&skipped)).err(),
		Some(RootError2::LineageGap)
	);
	// A replica whose first entry is not genesis (a snapshot joiner) is refused.
	assert_eq!(
		advance_pre_root2(&empty_authority2(), &adapt(&entries[1])).err(),
		Some(RootError2::LineageGap)
	);
}

/// Static only: the identity functions cannot fail for real payloads, so this pins the error
/// mapping's type and shape. It is not evidence of an exercised runtime failure.
#[test]
fn identity_error_is_typed() {
	let mapped: Result<(), AdapterError2> =
		Err(EntryIdentityError2::Serialize).map_err(AdapterError2::Identity);
	assert_eq!(
		mapped,
		Err(AdapterError2::Identity(EntryIdentityError2::Serialize))
	);
}

#[test]
fn carrier_legacy_wal_decoded_identities_are_frozen_for_versions_zero_through_four() {
	let oracle: serde_json::Value =
		serde_json::from_str(include_str!("wal_legacy_identity_vectors.json")).unwrap();
	let fixtures = [
		(
			0,
			include_str!("../../../../../tests/fixtures/consensus_body/wal_v4_entries.txt"),
		),
		(
			1,
			include_str!("../../../../../tests/fixtures/consensus_body/wal_v1_entries.txt"),
		),
		(
			2,
			include_str!("../../../../../tests/fixtures/consensus_body/wal_v2_entries.txt"),
		),
		(
			3,
			include_str!("../../../../../tests/fixtures/consensus_body/wal_v3_entries.txt"),
		),
		(
			4,
			include_str!("../../../../../tests/fixtures/consensus_body/wal_v4_entries.txt"),
		),
	];
	for (version, manifest) in fixtures {
		let mut count = 0;
		for line in manifest
			.lines()
			.filter(|line| !line.is_empty() && !line.starts_with('#'))
		{
			let (name, hex) = line.split_once('=').unwrap();
			let raw = unhex(hex);
			let entry = crate::consensus::raft_storage::carrier_legacy_fixture_entry(version, name)
				.unwrap();
			let expected = &oracle[version.to_string()][name];
			assert_eq!(
				hash(b"", &raw).as_slice(),
				unhex(expected["raw_sha256"].as_str().unwrap())
			);
			assert_eq!(
				postcard::to_stdvec(&entry).unwrap(),
				unhex(expected["decoded_current_hex"].as_str().unwrap()),
				"v{version} {name}: frozen upconversion"
			);
			let (_, identity, class) = adapt(&entry).entry_fields();
			assert!(class != EntryClass2::HeldCarrier);
			assert_eq!(
				identity.as_slice(),
				unhex(expected["identity"].as_str().unwrap()),
				"v{version} {name}: decoded-value identity"
			);
			count += 1;
		}
		assert_eq!(
			count,
			oracle[version.to_string()].as_object().unwrap().len()
		);
	}
}
#[test]
fn carrier_held_class_never_binds_trust_or_advances_origin() {
	let r = root("root_bootstrap_only");
	let c = command("command_bootstrap");
	for carrier in [
		crate::consensus::Root2Carrier1::new(
			&c.encode(r.state.generation, r.state.cluster).unwrap(),
			None,
		)
		.unwrap(),
		crate::consensus::Root2Carrier1::new(&[1], Some(&[2])).unwrap(),
	] {
		let entry = entry(
			log(1, 1, 1),
			EntryPayload::Normal(WendelonRequest::Root2Command(carrier)),
		);
		let ce = adapt(&entry);
		assert!(ce.entry_fields().2 == EntryClass2::HeldCarrier);
		let (position, identity, _) = ce.entry_fields();
		let _pollution=crate::storage::mutation_visibility::root2::committed::fixture::Root2FixtureScope::install(vec![(position,identity,EntryClass2::Bootstrap(c.digest(r.state.generation,r.state.cluster).unwrap()),c.clone())]);
		let ce = adapt(&entry);
		assert!(ce.entry_fields().2 == EntryClass2::HeldCarrier);
		assert_eq!(
			TrustedEntry2::bind(&ce, &c, r.state.generation, r.state.cluster).err(),
			Some(crate::storage::mutation_visibility::root2::trust::BindError2::NotRoot2Command)
		);
		let genesis = genesis(BTreeMap::from([(1, BasicNode::new("n1"))]));
		let pre = advance_pre_root2(&empty_authority2(), &adapt(&genesis))
			.unwrap()
			.0;
		let before = encode_authority2(&pre).unwrap();
		assert_eq!(
			advance_pre_root2(&pre, &ce).err(),
			Some(RootError2::TrustMismatch)
		);
		assert_eq!(encode_authority2(&pre).unwrap(), before);
	}
}
