//! `s7-measure compare`: arms are comparable only if every block ran the
//! same manifest over the same dataset (rows, order, schema) with the same
//! effective node configuration, except for the declared arm differences.

use serde_json::{Value, json};
use std::collections::BTreeMap;

/// The dataset identity every block must share.
pub const DATASET_KEYS: &[&str] = &[
	"generator",
	"scale_factor",
	"rows_total",
	"batch_rows",
	"batches_total",
	"last_batch_rows",
	"ipc_stream_sha256",
	"schema_sha256",
];

/// Per-block, never compared: paths and addresses, and the Debug dump that
/// embeds them (its structured fields are compared instead).
pub const PER_BLOCK_CONFIG: &[&str] = &["data_dir", "advertise_address", "l2", "debug_full"];

/// Allowed to differ between arms, and only between arms: the arm itself and
/// the one fix-only field.
pub const ARM_CONFIG: &[&str] = &["arm", "cfg_s7_fix", "tier_history_budget_bytes"];

/// One sealed block: its driver record and effective config.
pub struct Block {
	pub name: String,
	pub driver: Value,
	pub effective_config: Value,
}

fn config_view(cfg: &Value, skip_arm: bool) -> BTreeMap<String, Value> {
	cfg.as_object()
		.map(|o| {
			o.iter()
				.filter(|(k, _)| !PER_BLOCK_CONFIG.contains(&k.as_str()))
				.filter(|(k, _)| !(skip_arm && ARM_CONFIG.contains(&k.as_str())))
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect()
		})
		.unwrap_or_default()
}

/// Every incompatibility between `blocks`; empty means comparable.
pub fn compare(blocks: &[Block]) -> Vec<String> {
	let mut problems = Vec::new();
	let Some(first) = blocks.first() else {
		return vec!["no blocks".into()];
	};
	for b in blocks {
		if b.driver["dataset"].is_null() || b.effective_config.as_object().is_none() {
			problems.push(format!("{}: dataset or effective config missing", b.name));
		}
		if b.driver["manifest_sha256"].as_str().is_none() {
			problems.push(format!("{}: manifest sha missing", b.name));
		}
		if b.driver["manifest_sha256"] != first.driver["manifest_sha256"] {
			problems.push(format!("{}: manifest differs from {}", b.name, first.name));
		}
		for k in DATASET_KEYS {
			let (a, x) = (&first.driver["dataset"][k], &b.driver["dataset"][k]);
			if a.is_null() || a != x {
				problems.push(format!("{}: dataset {k} {x} != {} {a}", b.name, first.name));
			}
		}
		if b.driver["setup"]["descriptor"] != first.driver["setup"]["descriptor"] {
			problems.push(format!("{}: table descriptor differs", b.name));
		}
		if config_view(&b.effective_config, true) != config_view(&first.effective_config, true) {
			problems.push(format!(
				"{}: effective config differs beyond the arm fields",
				b.name
			));
		}
	}
	// Within an arm, even the arm fields must agree.
	let mut by_arm: BTreeMap<String, &Block> = BTreeMap::new();
	for b in blocks {
		let arm = b.effective_config["arm"]
			.as_str()
			.unwrap_or("?")
			.to_string();
		match by_arm.get(&arm) {
			None => {
				by_arm.insert(arm, b);
			}
			Some(a) => {
				if config_view(&a.effective_config, false)
					!= config_view(&b.effective_config, false)
				{
					problems.push(format!(
						"{}: config differs from {} within arm {arm}",
						b.name, a.name
					));
				}
			}
		}
	}
	problems
}

pub fn report(blocks: &[Block]) -> Value {
	let problems = compare(blocks);
	json!({
		"blocks": blocks.iter().map(|b| b.name.clone()).collect::<Vec<_>>(),
		"compatible": problems.is_empty(),
		"problems": problems,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	fn block(name: &str, arm: &str, sha: &str) -> Block {
		Block {
			name: name.into(),
			driver: json!({
				"manifest_sha256": "m1",
				"dataset": {
					"generator": "g", "scale_factor": 1.0, "rows_total": 6001215, "batch_rows": 4096,
					"batches_total": 1466, "last_batch_rows": 575,
					"ipc_stream_sha256": sha, "schema_sha256": "s1",
				},
				"setup": {"descriptor": "d"},
			}),
			effective_config: json!({
				"arm": arm, "cfg_s7_fix": arm == "fix",
				"tier_history_budget_bytes": if arm == "fix" { json!(268435456) } else { json!("N/A") },
				"slab_max_bytes": 4194304, "data_dir": format!("/x/{name}"),
				"advertise_address": format!("127.0.0.1:{}", name.len()), "debug_full": name,
				"tiering": {"scan_interval_ms": 1000},
			}),
		}
	}

	#[test]
	fn arms_with_the_same_dataset_and_config_are_comparable() {
		let blocks = [
			block("b1", "base", "h"),
			block("f2", "fix", "h"),
			block("f3", "fix", "h"),
		];
		assert_eq!(compare(&blocks), Vec::<String>::new());
	}

	#[test]
	fn a_different_dataset_hash_is_incompatible() {
		let blocks = [block("b1", "base", "h"), block("f2", "fix", "other")];
		assert!(
			compare(&blocks)
				.iter()
				.any(|p| p.contains("ipc_stream_sha256"))
		);
	}

	#[test]
	fn a_config_difference_beyond_the_arm_fields_is_incompatible() {
		let mut f = block("f2", "fix", "h");
		f.effective_config["slab_max_bytes"] = json!(1);
		assert!(
			compare(&[block("b1", "base", "h"), f])
				.iter()
				.any(|p| p.contains("effective config"))
		);
		let mut f3 = block("f3", "fix", "h");
		f3.effective_config["tier_history_budget_bytes"] = json!(1);
		assert!(
			compare(&[block("f2", "fix", "h"), f3])
				.iter()
				.any(|p| p.contains("within arm")),
			"the fix-only field must agree within the fix arm"
		);
	}

	#[test]
	fn missing_identity_is_incompatible_not_equal() {
		let mut a = block("b1", "base", "h");
		let mut b = block("f2", "fix", "h");
		a.driver["manifest_sha256"] = Value::Null;
		b.driver["manifest_sha256"] = Value::Null;
		a.driver["dataset"]["ipc_stream_sha256"] = Value::Null;
		b.driver["dataset"]["ipc_stream_sha256"] = Value::Null;
		let p = compare(&[a, b]);
		assert!(
			p.iter().any(|p| p.contains("manifest sha missing")),
			"{p:?}"
		);
		assert!(p.iter().any(|p| p.contains("ipc_stream_sha256")), "{p:?}");
	}
}
