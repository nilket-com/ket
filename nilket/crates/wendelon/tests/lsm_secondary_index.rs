use std::collections::BTreeSet;
use std::fs;
use syn::{ImplItem, Item};

fn parse_rust_file(path: &str) -> syn::File {
	let source = fs::read_to_string(path).unwrap();
	syn::parse_file(&source).unwrap_or_else(|err| panic!("failed to parse {path}: {err}"))
}

fn function_names(path: &str) -> BTreeSet<String> {
	let file = parse_rust_file(path);
	let mut names = BTreeSet::new();
	collect_function_names(&file.items, &mut names);
	names
}

fn collect_function_names(items: &[Item], names: &mut BTreeSet<String>) {
	for item in items {
		match item {
			Item::Fn(func) => {
				names.insert(func.sig.ident.to_string());
			}
			Item::Impl(imp) => {
				for impl_item in &imp.items {
					if let ImplItem::Fn(method) = impl_item {
						names.insert(method.sig.ident.to_string());
					}
				}
			}
			Item::Mod(module) => {
				if let Some((_, items)) = &module.content {
					collect_function_names(items, names);
				}
			}
			_ => {}
		}
	}
}

fn enum_variants(path: &str, enum_name: &str) -> BTreeSet<String> {
	let file = parse_rust_file(path);
	let mut variants = BTreeSet::new();
	collect_enum_variants(&file.items, enum_name, &mut variants);
	variants
}

fn collect_enum_variants(items: &[Item], enum_name: &str, variants: &mut BTreeSet<String>) {
	for item in items {
		match item {
			Item::Enum(item_enum) if item_enum.ident == enum_name => {
				for variant in &item_enum.variants {
					variants.insert(variant.ident.to_string());
				}
			}
			Item::Mod(module) => {
				if let Some((_, items)) = &module.content {
					collect_enum_variants(items, enum_name, variants);
				}
			}
			_ => {}
		}
	}
}

#[test]
fn lsm_secondary_index_compaction_path_has_behavioral_guards() {
	let manager_functions = function_names("src/storage/manager.rs");

	for required in [
		"prepare_lsm_secondary_index_segments_for_compaction",
		"publish_prepared_lsm_secondary_index_segments",
		"update_lsm_sidecar_coverage_after_publish",
	] {
		assert!(
			manager_functions.contains(required),
			"src/storage/manager.rs missing LSM secondary-index sidecar function {required}"
		);
	}

	for required in [
		"lsm_compaction_rebuilds_secondary_index_sidecar_with_catalog_context",
		"lsm_compaction_commit_failure_does_not_publish_prepared_sidecar",
		"lsm_compaction_rejects_secondary_index_sidecar_without_catalog_context",
	] {
		assert!(
			manager_functions.contains(required),
			"src/storage/manager.rs missing secondary-index behavior test {required}"
		);
	}
}

#[test]
fn lsm_secondary_index_compaction_rejects_in_place_sidecar_swaps() {
	let lsm_reject_variants = enum_variants("src/storage/lsm.rs", "LsmRejectCode");
	let manager_functions = function_names("src/storage/manager.rs");
	let manager_manifest_test_functions = function_names("tests/lsm_manager_manifest.rs");

	assert!(lsm_reject_variants.contains("SidecarDigestMismatch"));
	assert!(manager_functions.contains("validate_lsm_sidecar_swap_readiness"));
	assert!(
		manager_manifest_test_functions
			.contains("manager_install_rejects_explicit_lsm_sidecar_swaps")
	);

	let forbidden = manager_functions
		.iter()
		.filter(|name| {
			let name = name.as_str();
			name.contains("sidecar")
				&& name.contains("in_place")
				&& (name.contains("rewrite")
					|| name.contains("mutate")
					|| name.contains("overwrite"))
		})
		.collect::<Vec<_>>();
	assert!(
		forbidden.is_empty(),
		"LSM secondary-index compaction must publish replacement sidecars, not rewrite in place: {forbidden:?}"
	);
}
