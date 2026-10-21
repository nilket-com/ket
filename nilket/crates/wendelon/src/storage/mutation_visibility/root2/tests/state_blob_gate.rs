//! G6 (completion gate, c2): every VISAUT04/05/06 state blob in the root2 JSON vectors, nested ones
//! included, decodes and is committed to the profile-3 manifest length (`control_len` 241).
//! `tests/removed_forms.json` (the removed forms) is excluded; VISAUT02/03 vectors are excluded by
//! magic. The per-file blob census is exact.
use super::*;
use std::collections::BTreeMap;
use std::path::Path;

fn strings(v: &serde_json::Value, out: &mut Vec<String>) {
	match v {
		serde_json::Value::String(s) => out.push(s.clone()),
		serde_json::Value::Array(a) => a.iter().for_each(|x| strings(x, out)),
		serde_json::Value::Object(o) => o.values().for_each(|x| strings(x, out)),
		_ => {}
	}
}
fn hex_bytes(s: &str) -> Option<Vec<u8>> {
	if s.len() % 2 != 0 || !s.bytes().all(|c| c.is_ascii_hexdigit()) {
		return None;
	}
	Some(
		(0..s.len())
			.step_by(2)
			.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
			.collect(),
	)
}
fn json_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
	for e in std::fs::read_dir(dir).unwrap() {
		let p = e.unwrap().path();
		if p.is_dir() {
			json_files(&p, out);
		} else if p.extension().is_some_and(|x| x == "json") {
			out.push(p);
		}
	}
}
/// Every state blob in `bytes`: (version digit, control_len), each decoded.
fn blobs(bytes: &[u8]) -> Vec<(char, u64)> {
	let mut found = Vec::new();
	for at in 0..bytes.len().saturating_sub(8) {
		let magic = &bytes[at..at + 8];
		let Some(version) = [b"VISAUT04", b"VISAUT05", b"VISAUT06"]
			.iter()
			.position(|m| magic == *m)
		else {
			continue;
		};
		let len = u32::from_le_bytes(bytes[at + 12..at + 16].try_into().unwrap()) as usize;
		let envelope = &bytes[at..at + 16 + len + 32];
		UnverifiedRoot2::decode(envelope)
			.unwrap_or_else(|e| panic!("a state blob at offset {at} does not decode: {e:?}"));
		let control_len = u64::from_le_bytes(envelope[192..200].try_into().unwrap());
		found.push((char::from(b'4' + version as u8), control_len));
	}
	found
}
#[test]
fn every_json_state_blob_is_committed_to_241() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/storage/mutation_visibility/root2");
	let mut files = Vec::new();
	json_files(&root, &mut files);
	let mut census: BTreeMap<String, Vec<char>> = BTreeMap::new();
	for path in files {
		let rel = path
			.strip_prefix(&root)
			.unwrap()
			.to_string_lossy()
			.into_owned();
		if rel == "tests/removed_forms.json" {
			continue;
		}
		let v: serde_json::Value =
			serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
		let mut all = Vec::new();
		strings(&v, &mut all);
		for s in all {
			let Some(bytes) = hex_bytes(&s) else { continue };
			for (version, control_len) in blobs(&bytes) {
				assert_eq!(control_len, 241, "{rel}: a V{version} blob");
				census.entry(rel.clone()).or_default().push(version);
			}
		}
	}
	let census: Vec<(String, String)> = census
		.into_iter()
		.map(|(f, mut v)| {
			v.sort();
			(f, v.into_iter().collect())
		})
		.collect();
	let expected = [
		("tests/gate_vectors.json", "4"),
		("tests/goldens.json", "444444444"),
		("tests/origin_vectors.json", "4"),
		("tests/retirement/owner_binding/vectors.json", "66666666"),
		("tests/retirement/vectors.json", "6666"),
		("tests/role_epochs/vectors.json", "45"),
		("tests/role_freeze/vectors.json", "555"),
		("tests/root_profile_vectors.json", "4"),
	]
	.map(|(f, v)| (f.to_string(), v.to_string()));
	assert_eq!(census, expected);
	// Probe: a blob committed to another length is found and reported with its value.
	let mut b = fixture("root_bootstrap_only");
	b[192..200].copy_from_slice(&240u64.to_le_bytes());
	assert!(UnverifiedRoot2::decode(&b).is_err());
	assert_eq!(blobs(&fixture("root_bootstrap_only")), [('4', 241)]);
}
