//! Architecture guards for the clean post-census TORON plan namespace.

use std::collections::BTreeMap;
use std::path::Path;

use super::support::manifest_dir;

const FIRST_GUARDED_ID: u32 = 528;

#[test]
fn post_census_adr_and_impl_slugs_match() {
	let plans = manifest_dir().join("plans");
	let adr = markdown_names(&plans.join("adr"));
	let implementation = markdown_names(&plans.join("impl"));
	let errors = namespace_mismatches(&adr, &implementation);

	assert!(
		errors.is_empty(),
		"post-census TORON ADR/implementation slugs diverged:\n{}\n\
		 see plans/impl/535_global_sort_barrier_completion.md",
		errors.join("\n")
	);
}

#[test]
fn post_census_namespace_guard_rejects_a_mismatched_pair() {
	let errors = namespace_mismatches(
		&["535_global_sort_barrier_completion.md".to_string()],
		&["535_sort_barrier_other_name.md".to_string()],
	);

	assert_eq!(errors.len(), 1, "positive control must trip the guard");
	assert!(errors[0].contains("global_sort_barrier_completion"));
	assert!(errors[0].contains("sort_barrier_other_name"));
}

#[test]
fn implementation_only_ids_and_historical_companions_are_valid() {
	let errors = namespace_mismatches(
		&[
			"527_old_name.md".to_string(),
			"528_fragment_capability_census.md".to_string(),
		],
		&[
			"527_different_old_name.md".to_string(),
			"528_fragment_capability_census.md".to_string(),
			"528_fragment_capability_census_results.md".to_string(),
			"530_dogfood_loop.md".to_string(),
		],
	);

	assert!(errors.is_empty(), "valid namespace forms: {errors:?}");
}

fn markdown_names(directory: &Path) -> Vec<String> {
	let mut names = std::fs::read_dir(directory)
		.unwrap_or_else(|error| panic!("read {}: {error}", directory.display()))
		.filter_map(|entry| {
			let entry = entry.expect("read plan directory entry");
			entry
				.file_type()
				.expect("read plan entry type")
				.is_file()
				.then(|| entry.file_name().to_string_lossy().into_owned())
		})
		.filter(|name| name.ends_with(".md"))
		.collect::<Vec<_>>();
	names.sort();
	names
}

fn namespace_mismatches(adr_names: &[String], impl_names: &[String]) -> Vec<String> {
	let adr = canonical_by_id(adr_names);
	let implementation = canonical_by_id(impl_names);
	let mut errors = Vec::new();

	for (id, adr_slugs) in adr {
		if id < FIRST_GUARDED_ID {
			continue;
		}
		let Some(impl_slugs) = implementation.get(&id) else {
			continue;
		};
		if adr_slugs.len() != 1 || impl_slugs.len() != 1 || adr_slugs[0] != impl_slugs[0] {
			errors.push(format!(
				"TORON {id}: ADR slug(s) {} != implementation slug(s) {}",
				adr_slugs.join(", "),
				impl_slugs.join(", ")
			));
		}
	}

	errors
}

fn canonical_by_id(names: &[String]) -> BTreeMap<u32, Vec<String>> {
	let mut by_id = BTreeMap::<u32, Vec<String>>::new();
	for name in names {
		let Some((id, slug)) = parse_plan_name(name) else {
			continue;
		};
		if is_historical_companion(&slug) {
			continue;
		}
		by_id.entry(id).or_default().push(slug);
	}
	by_id
}

fn parse_plan_name(name: &str) -> Option<(u32, String)> {
	let stem = name.strip_suffix(".md")?;
	let (id, slug) = stem.split_once('_')?;
	if slug.is_empty() {
		return None;
	}
	Some((id.parse().ok()?, slug.to_string()))
}

fn is_historical_companion(slug: &str) -> bool {
	slug.ends_with("_results")
		|| slug.contains("_results_")
		|| slug.ends_with("_evidence")
		|| slug.contains("_evidence_")
}
