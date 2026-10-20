//! T-FEATURE: the `fixture-anchor` feature reaches no shipped build. Over the workspace's
//! `cargo metadata --no-deps`, no package enables it on a normal or build edge, no package feature
//! forwards it, key-guard's own features never imply it, and the one edge allowed to enable it is
//! `wendelon-key`'s dev-dependency. Resolver 3 keeps dev-dependency features out of every
//! non-test build, so no shipped binary links the anchored open.
use serde_json::{Value, json};
use std::path::Path;
use std::process::Command;

const FEATURE: &str = "fixture-anchor";
const ALLOWED: &str = "wendelon-key";

fn strings(value: &Value) -> impl Iterator<Item = &str> {
	value
		.as_array()
		.into_iter()
		.flatten()
		.filter_map(Value::as_str)
}

/// Whether a feature-table entry turns on key-guard's anchor: `<dep>/fixture-anchor` or
/// `<dep>?/fixture-anchor`, where `<dep>` is key-guard or any name the package renamed it to.
fn forwards(entry: &str, aliases: &[&str]) -> bool {
	entry
		.split_once('/')
		.is_some_and(|(dep, f)| aliases.contains(&dep.trim_end_matches('?')) && f == FEATURE)
}

/// Every violation in `metadata`; empty when the feature is confined.
fn violations(metadata: &Value) -> Vec<String> {
	let mut out = Vec::new();
	for package in strings_of(metadata, "packages") {
		let name = package["name"].as_str().unwrap_or("?");
		let aliases: Vec<&str> = std::iter::once("key-guard")
			.chain(
				strings_of(package, "dependencies")
					.filter(|d| d["name"] == "key-guard")
					.filter_map(|d| d["rename"].as_str()),
			)
			.collect();
		for (feature, implies) in package["features"].as_object().into_iter().flatten() {
			for entry in strings(implies) {
				let own = name == "key-guard" && feature != FEATURE && entry == FEATURE;
				if own || forwards(entry, &aliases) {
					out.push(format!("{name}: feature {feature} turns on {entry}"));
				}
			}
		}
		for dep in package["dependencies"].as_array().into_iter().flatten() {
			if dep["name"] != "key-guard" || !strings(&dep["features"]).any(|f| f == FEATURE) {
				continue;
			}
			let kind = dep["kind"].as_str().unwrap_or("normal");
			if kind != "dev" || name != ALLOWED {
				out.push(format!(
					"{name}: a {kind} edge to key-guard enables {FEATURE}"
				));
			}
		}
	}
	out
}

fn strings_of<'a>(value: &'a Value, key: &str) -> impl Iterator<Item = &'a Value> {
	value[key].as_array().into_iter().flatten()
}

fn workspace_metadata() -> Value {
	let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.toml");
	let output = Command::new(env!("CARGO"))
		.args([
			"metadata",
			"--format-version",
			"1",
			"--no-deps",
			"--offline",
		])
		.arg("--manifest-path")
		.arg(&workspace)
		.output()
		.expect("cargo metadata runs");
	assert!(
		output.status.success(),
		"{}",
		String::from_utf8_lossy(&output.stderr)
	);
	serde_json::from_slice(&output.stdout).expect("cargo metadata is JSON")
}

#[test]
fn the_fixture_anchor_reaches_no_shipped_build() {
	let metadata = workspace_metadata();
	let names: Vec<&str> = strings_of(&metadata, "packages")
		.filter_map(|p| p["name"].as_str())
		.collect();
	// The scan is not vacuous: it sees key-guard, which declares the feature, and its users.
	for expected in ["key-guard", "tls-pki", "wendelon"] {
		assert!(names.contains(&expected), "{expected} in {names:?}");
	}
	let key_guard = strings_of(&metadata, "packages")
		.find(|p| p["name"] == "key-guard")
		.unwrap();
	assert_eq!(key_guard["features"][FEATURE], json!([]));
	assert_eq!(violations(&metadata), Vec::<String>::new());
}

fn package(name: &str, features: Value, deps: Value) -> Value {
	json!({ "name": name, "features": features, "dependencies": deps })
}

fn edge(kind: Option<&str>, features: &[&str]) -> Value {
	json!({ "name": "key-guard", "kind": kind, "features": features })
}

#[test]
fn the_feature_check_refuses_every_way_in_and_admits_the_one_dev_edge() {
	let key_guard = |default: &[&str]| {
		package(
			"key-guard",
			json!({ "default": default, FEATURE: [] }),
			json!([]),
		)
	};
	let with = |extra: Value| json!({ "packages": [key_guard(&[]), extra] });
	// Control: the one admitted edge, and a plain normal edge without the feature.
	let admitted = with(package(
		ALLOWED,
		json!({}),
		json!([edge(Some("dev"), &[FEATURE])]),
	));
	assert!(violations(&admitted).is_empty());
	let plain = with(package("tls-pki", json!({}), json!([edge(None, &[])])));
	assert!(violations(&plain).is_empty());
	// Control: a renamed dependency whose name is used for another feature is not a forward.
	let renamed = with(package(
		"wendelon",
		json!({ "x": ["kg/other-feature", "other/fixture-anchor"] }),
		json!([{ "name": "key-guard", "rename": "kg", "kind": null, "features": [] }]),
	));
	assert!(violations(&renamed).is_empty());
	for (what, metadata) in [
		(
			"normal edge",
			with(package(
				"tls-pki",
				json!({}),
				json!([edge(None, &[FEATURE])]),
			)),
		),
		(
			"build edge",
			with(package(
				ALLOWED,
				json!({}),
				json!([edge(Some("build"), &[FEATURE])]),
			)),
		),
		(
			"wendelon-key normal edge",
			with(package(ALLOWED, json!({}), json!([edge(None, &[FEATURE])]))),
		),
		(
			"another package's dev edge",
			with(package(
				"wendelon",
				json!({}),
				json!([edge(Some("dev"), &[FEATURE])]),
			)),
		),
		(
			"forwarding feature",
			with(package(
				"wendelon",
				json!({ "test-support": ["key-guard/fixture-anchor"] }),
				json!([edge(None, &[])]),
			)),
		),
		(
			"weak forwarding feature",
			with(package(
				ALLOWED,
				json!({ "x": ["key-guard?/fixture-anchor"] }),
				json!([]),
			)),
		),
		(
			"forwarding through a renamed dependency",
			with(package(
				"wendelon",
				json!({ "x": ["kg/fixture-anchor"] }),
				json!([{ "name": "key-guard", "rename": "kg", "kind": null, "features": [] }]),
			)),
		),
		(
			"weak forwarding through a renamed dependency",
			with(package(
				ALLOWED,
				json!({ "x": ["kg?/fixture-anchor"] }),
				json!([{ "name": "key-guard", "rename": "kg", "kind": "dev", "features": [] }]),
			)),
		),
		("default", json!({ "packages": [key_guard(&[FEATURE])] })),
	] {
		assert!(!violations(&metadata).is_empty(), "{what}");
	}
}
