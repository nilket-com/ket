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

/// Per-block, never compared: paths and addresses, and a legacy record's Debug dump that
/// embeds them. DC-309: new writers omit the dump; it is ignored on input and never read, so its
/// text never reaches a comparison or its output.
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

	/// DC-309: a synthetic stand-in for a legacy record's Debug dump, never a real one.
	const LEGACY_DUMP_SENTINEL: &str = "dc309-legacy-dump-sentinel";

	/// A record as written before DC-309: a Debug dump embedding the block's own paths, `l2` as a
	/// Debug string, and none of the fields the new writer adds.
	fn legacy(name: &str, arm: &str) -> Block {
		let mut b = block(name, arm, "h");
		b.effective_config["debug_full"] = json!(format!(
			"{LEGACY_DUMP_SENTINEL} {{ data_dir: \"/x/{name}/store\", l2: \"/x/{name}/l2\" }}"
		));
		b.effective_config["l2"] = json!(format!("Some(LocalFs {{ root: \"/x/{name}/l2\" }})"));
		b
	}

	/// A record as the DC-309 writer produces it: no dump, the added provenance fields.
	fn current(name: &str, arm: &str) -> Block {
		let mut b = block(name, arm, "h");
		let config = b.effective_config.as_object_mut().unwrap();
		config.remove("debug_full");
		config.insert(
			"l2".into(),
			json!({"kind": "local_fs", "root": format!("/x/{name}/l2")}),
		);
		config.insert("allow_dev_keys".into(), json!(true));
		config.insert("wal_io_depth".into(), json!(64));
		config.insert("cluster".into(), json!({"mode": "bootstrap"}));
		b
	}

	/// No comparison result or report carries any of a legacy dump's text.
	fn no_dump_text(blocks: &[Block]) -> bool {
		let out = serde_json::to_string(&report(blocks)).unwrap();
		!out.contains(LEGACY_DUMP_SENTINEL) && !out.contains("/store")
	}

	/// Legacy records that differ only in their dumps (per-block paths) stay comparable: the
	/// dump is ignored on input, never compared and never echoed.
	#[test]
	fn legacy_records_differing_only_in_their_dumps_are_comparable() {
		let blocks = [
			legacy("b1", "base"),
			legacy("f2", "fix"),
			legacy("f3", "fix"),
		];
		assert!(
			blocks[1].effective_config["debug_full"] != blocks[2].effective_config["debug_full"],
			"the fixture's dumps differ"
		);
		assert!(
			compare(&blocks).is_empty(),
			"legacy records differing only in their dumps are refused"
		);
		assert!(no_dump_text(&blocks), "a legacy dump reached the output");
	}

	/// Control: a real compared field still separates legacy records, across and within arms,
	/// and the refusal carries no dump text.
	#[test]
	fn a_legacy_record_differing_in_a_compared_field_is_still_refused() {
		let mut f3 = legacy("f3", "fix");
		f3.effective_config["slab_max_bytes"] = json!(1);
		let within = [legacy("f2", "fix"), f3];
		assert!(
			compare(&within).iter().any(|p| p.contains("within arm")),
			"a compared difference within an arm was accepted"
		);
		assert!(no_dump_text(&within), "a legacy dump reached the output");
		let mut b1 = legacy("b1", "base");
		b1.effective_config["slab_max_bytes"] = json!(1);
		let across = [b1, legacy("f2", "fix")];
		assert!(
			compare(&across)
				.iter()
				.any(|p| p.contains("effective config differs beyond the arm fields")),
			"a compared difference across arms was accepted"
		);
		assert!(no_dump_text(&across), "a legacy dump reached the output");
	}

	/// Mixed and incomplete provenance is never declared equivalent. A field present in one
	/// record and missing from the other is a difference, never a default: a legacy record's value
	/// for it exists only inside its dump, which is not read, so equality cannot be shown.
	#[test]
	fn mixed_or_missing_provenance_is_not_declared_equivalent() {
		let mixed = [legacy("b1", "base"), current("f2", "fix")];
		assert!(
			compare(&mixed)
				.iter()
				.any(|p| p.contains("effective config differs beyond the arm fields")),
			"a legacy and a current record were declared equivalent"
		);
		let same_arm = [legacy("f2", "fix"), current("f3", "fix")];
		assert!(
			compare(&same_arm).iter().any(|p| p.contains("within arm")),
			"a legacy and a current record were declared equivalent within an arm"
		);
		assert!(
			no_dump_text(&mixed) && no_dump_text(&same_arm),
			"a legacy dump reached the output"
		);
		let mut missing = current("f3", "fix");
		missing
			.effective_config
			.as_object_mut()
			.unwrap()
			.remove("allow_dev_keys");
		assert!(
			compare(&[current("f2", "fix"), missing])
				.iter()
				.any(|p| p.contains("within arm")),
			"a record missing a provenance field was declared equivalent"
		);
		let mut differing = current("b1", "base");
		differing.effective_config["wal_io_depth"] = json!(8);
		assert!(
			!compare(&[differing, current("f2", "fix")]).is_empty(),
			"records with different provenance were declared equivalent"
		);
		assert!(
			compare(&[
				current("b1", "base"),
				current("f2", "fix"),
				current("f3", "fix")
			])
			.is_empty(),
			"current records with equal provenance are refused"
		);
	}
}
