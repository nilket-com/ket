use super::errors::AdminManifestError2;
use super::key::check_enrollable_admin_key2;
use super::manifest::*;
mod negative;
mod vectors;

fn hex(s: &str) -> Vec<u8> {
	assert_eq!(s.len() % 2, 0);
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
fn vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("vectors.json")).unwrap()
}
fn valid(name: &str) -> serde_json::Value {
	vectors()["valid"]
		.as_array()
		.unwrap()
		.iter()
		.find(|v| v["name"] == name)
		.unwrap()
		.clone()
}
fn bytes(name: &str) -> Vec<u8> {
	hex(valid(name)["bytes"].as_str().unwrap())
}
